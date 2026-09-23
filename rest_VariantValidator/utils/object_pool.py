import contextlib
import threading
import logging
import platform
import gc
import resource
from configparser import ConfigParser

import psutil
import mysql.connector

from VariantValidator import Validator, settings as vv_settings
from VariantFormatter.simpleVariantFormatter import SimpleVariantFormatter

class ObjectPoolTimeoutException(Exception):
    pass

# -----------------------------------------------------------------------------
# Logger
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# =============================================================================
# UTILS
# =============================================================================

def _total_ram_mb():
    return psutil.virtual_memory().total // (1024 * 1024)


# =============================================================================
# MEMORY ESTIMATION
# =============================================================================

def _estimate_rss_mb(factory, safety_factor=2.0):
    """
    Generic RSS delta estimator for a callable factory.
    """
    gc.collect()
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    obj = factory()
    del obj

    gc.collect()
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    delta = max(after - before, 0)

    # macOS reports bytes, Linux reports KB
    if platform.system() == "Darwin":
        delta_mb = delta / (1024 * 1024)
    else:
        delta_mb = delta / 1024

    estimate = int(delta_mb * safety_factor)
    return max(estimate, 0)


def _estimate_validator_mem_mb():
    mem = _estimate_rss_mb(Validator, safety_factor=2.0)
    if mem <= 0:
        logger.warning(
            "Validator memory estimate unreliable; defaulting to 64 MB"
        )
        return 64
    return max(mem, 64)


def _estimate_formatter_mem_mb(validator_cost):
    """
    Formatter MUST cost at least as much as a Validator,
    because it owns a Validator internally.
    """
    mem = _estimate_rss_mb(SimpleVariantFormatter, safety_factor=2.0)
    if mem <= 0:
        logger.warning(
            "Formatter memory estimate unreliable; using Validator cost"
        )
        return validator_cost
    return max(mem, validator_cost)


# =============================================================================
# MYSQL
# =============================================================================

def _mysql_max_connections_from_config():
    config = ConfigParser()
    config.read(vv_settings.get_config_dir())

    if not config.has_section("mysql"):
        raise RuntimeError("No [mysql] section found in VariantValidator config")

    conn = mysql.connector.connect(
        host=config.get("mysql", "host"),
        port=config.getint("mysql", "port"),
        user=config.get("mysql", "user"),
        password=config.get("mysql", "password"),
        database=config.get("mysql", "database"),
    )

    try:
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE 'max_connections'")
        _, value = cur.fetchone()
        return int(value)
    finally:
        conn.close()


# =============================================================================
# CAPACITY COMPUTATION (RAM + MYSQL, WEIGHTED)
# =============================================================================

def compute_pool_sizes():
    total_ram = _total_ram_mb()
    usable_ram = int(total_ram * 0.45)  # policy

    validator_cost = _estimate_validator_mem_mb()
    formatter_cost = _estimate_formatter_mem_mb(validator_cost)

    mysql_max = _mysql_max_connections_from_config()
    mysql_reserved = max(10, int(mysql_max * 0.15))
    mysql_limit = max(mysql_max - mysql_reserved, 1)

    # Allocation ratios
    VVAL_RATIO = 0.4
    VF_RATIO   = 0.4

    ram_for_vval = int(usable_ram * VVAL_RATIO)
    ram_for_vf   = int(usable_ram * VF_RATIO)
    ram_for_g2t  = usable_ram - ram_for_vval - ram_for_vf

    vval_size = max(1, ram_for_vval // validator_cost)
    vf_size   = max(1, ram_for_vf   // formatter_cost)
    g2t_size  = max(1, ram_for_g2t  // validator_cost)

    total_units = vval_size + vf_size + g2t_size

    # Enforce MySQL ceiling
    if total_units > mysql_limit:
        scale = mysql_limit / float(total_units)
        vval_size = max(1, int(vval_size * scale))
        vf_size   = max(1, int(vf_size * scale))
        g2t_size  = max(1, mysql_limit - vval_size - vf_size)

    logger.warning(
        "Capacity scan:"
        " RAM=%dMB (usable=%dMB), "
        "validator_cost=%dMB, "
        "formatter_cost=%dMB, "
        "mysql_usable=%d",
        total_ram,
        usable_ram,
        validator_cost,
        formatter_cost,
        mysql_limit,
    )

    logger.warning(
        "Pool sizes:"
        " vval=%d, vf=%d, g2t=%d (total=%d)",
        vval_size,
        vf_size,
        g2t_size,
        vval_size + vf_size + g2t_size,
    )

    return vval_size, vf_size, g2t_size


# =============================================================================
# OBJECT POOL (leak-safe: checkout via context manager)
# =============================================================================

class ObjectPool:
    def __init__(self, factory, initial_pool_size=0, max_pool_size=10):
        self.factory = factory
        self.max_pool_size = max_pool_size
        self._pool_size = initial_pool_size
        self._available = [factory() for _ in range(initial_pool_size)]
        self._pool_size = len(self._available)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)

    def __len__(self):
        with self._lock:
            return self._pool_size

    def total(self):
        return self._pool_size

    def ensure(self, min_size):
        """Ensure that at least min_size items are available in the pool."""
        if min_size > self.max_pool_size:
            raise ValueError("min_size cannot be greater than max_pool_size")
        with self._condition:
            if self._pool_size >= min_size:
                return
            need = min_size - self._pool_size
            to_add = [self.factory() for _ in range(need)]
            self._available.extend(to_add)
            self._pool_size += len(to_add)
            self._condition.notify(need)

    @contextlib.contextmanager
    def item(self, timeout=None):
        """Check out an item from the pool and ensure it is returned.

        This must be used as a context manager for reliable cleanup:

            with pool.item() as obj:
                # use obj
                pass
        """

        with self._condition:
            if len(self._available) > 0:
                obj = self._available.pop()
            elif self._pool_size < self.max_pool_size:
                # Create a new object to add to the pool
                obj = self.factory()
                self._pool_size += 1
            else:
                if not self._condition.wait_for(lambda: len(self._available) > 0,
                                                timeout=timeout):
                    raise ObjectPoolTimeoutException("Timeout waiting for object from pool")
                obj = self._available.pop()
            try:
                yield obj
            finally:
                # return the object to the pool
                self._available.append(obj)
                self._condition.notify()


# =============================================================================
# POOLS (EXPORT NAMES EXPECTED BY ENDPOINTS)
# =============================================================================

vval_size, vf_size, g2t_size = compute_pool_sizes()

# Validator-only pools
vval_object_pool = ObjectPool(Validator, initial_pool_size=vval_size, max_pool_size=vval_size)
g2t_object_pool = ObjectPool(Validator, initial_pool_size=g2t_size, max_pool_size=g2t_size)

# Formatter pool (OBJECT MODE ONLY)
simple_variant_formatter_pool = ObjectPool(SimpleVariantFormatter, initial_pool_size=vf_size, max_pool_size=vf_size)


# -----------------------------------------------------------------------------
# FINAL LOGGING
# -----------------------------------------------------------------------------
logger.warning("Object pools initialised:")

logger.warning("  vval_object_pool: %d", vval_object_pool.total())
logger.warning("  simple_variant_formatter_pool: %d", simple_variant_formatter_pool.total())
logger.warning("  g2t_object_pool: %d", g2t_object_pool.total())

logger.warning(
    "Total Validator-weighted capacity: %d",
    vval_object_pool.total()
    + simple_variant_formatter_pool.total()
    + g2t_object_pool.total(),
)


# <LICENSE>
# Copyright (C) 2016-2026 VariantValidator Contributors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# </LICENSE>
