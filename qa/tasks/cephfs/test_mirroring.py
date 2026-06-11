import os
import re
import json
import base64
import errno
import logging
import random
import signal
import time
import functools

from io import StringIO
from collections import deque

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while
from teuthology.orchestra import run

log = logging.getLogger(__name__)


# Exceptions to retry in test assertions
RETRY_EXCEPTIONS = (AssertionError, KeyError, IndexError, CommandFailedError)
# retry decorator
def retry_assert(timeout=60, interval=1):
    """
    Retry a test helper until assertions inside it pass or timeout expires.
    Prints retry count on each failure.
    """
    tries = int(timeout/interval)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            attempt = 1

            with safe_while(sleep=interval, tries=tries, action=f"retry {func.__name__}") as proceed:
                while proceed():
                    try:
                        return func(*args, **kwargs)
                    except RETRY_EXCEPTIONS as e:
                        last_exc = e
                        log.debug(
                            f"[retry_assert] {func.__name__}: "
                            f"attempt {attempt} failed ({type(e).__name__}), retrying..."
                        )
                        attempt += 1
            # Final failure
            if last_exc is not None and hasattr(last_exc, "res"):
                log.error("\n--- Last peer status (res) ---")
                log.error(last_exc.res)

            raise AssertionError(
                f"{func.__name__} did not succeed within {timeout}s "
                f"after {attempt - 1} attempts"
            ) from last_exc

        return wrapper
    return decorator


class TestMirroring(CephFSTestCase):
    MDSS_REQUIRED = 5
    CLIENTS_REQUIRED = 2
    REQUIRE_BACKUP_FILESYSTEM = True

    MODULE_NAME = "mirroring"

    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR = "cephfs_mirror"
    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS = "cephfs_mirror_mirrored_filesystems"
    PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_PEER = "cephfs_mirror_peers"
    MGR_METRICS_CACHE_TTL = 3
    MIRROR_LIVE_METRICS_PERSIST_INTERVAL = 1

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.primary_fs_id = self.fs.id
        self.secondary_fs_name = self.backup_fs.name
        self.secondary_fs_id = self.backup_fs.id
        self.enable_mirroring_module()
        self.config_set('client.mirror', 'cephfs_mirror_directory_scan_interval', 1)
        self.config_set('client.mirror', 'cephfs_mirror_live_metrics_persist_interval',
                          self.MIRROR_LIVE_METRICS_PERSIST_INTERVAL)
        self.config_set('mgr', 'mgr/mirroring/snapshot_mirror_metrics_cache_ttl',
                          self.MGR_METRICS_CACHE_TTL)

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.run_ceph_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        self.assertEqual(res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]["labels"]["filesystem"],
                         fs_name)
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertGreater(vafter["counters"]["mirrored_filesystems"],
                           vbefore["counters"]["mirrored_filesystems"])

    def disable_mirroring(self, fs_name, fs_id):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.run_ceph_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                       'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

        # verify labelled perf counter
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR][0]

        self.assertLess(vafter["counters"]["mirrored_filesystems"],
                        vbefore["counters"]["mirrored_filesystems"])

    def verify_peer_added(self, fs_name, fs_id, peer_spec, remote_fs_name=None):
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.assertTrue(peer_uuid in res['peers'])
        client_name = res['peers'][peer_uuid]['remote']['client_name']
        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
        if remote_fs_name:
            self.assertTrue(self.secondary_fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
        else:
            self.assertTrue(self.fs_name == res['peers'][peer_uuid]['remote']['fs_name'])

    def peer_add(self, fs_name, fs_id, peer_spec, remote_fs_name=None, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        if remote_fs_name:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec, remote_fs_name)
        else:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec)
        time.sleep(10)
        self.verify_peer_added(fs_name, fs_id, peer_spec, remote_fs_name)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["mirroring_peers"], vbefore["counters"]["mirroring_peers"])

    def peer_remove(self, fs_name, fs_id, peer_spec):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        peer_uuid = self.get_peer_uuid(peer_spec)
        self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_remove", fs_name, peer_uuid)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {} and res['snap_dirs']['dir_count'] == 0)

        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        self.assertLess(vafter["counters"]["mirroring_peers"], vbefore["counters"]["mirroring_peers"])

    def bootstrap_peer(self, fs_name, client_name, site_name):
        outj = json.loads(self.get_ceph_cmd_stdout(
            "fs", "snapshot", "mirror", "peer_bootstrap", "create", fs_name,
            client_name, site_name))
        return outj['token']

    def import_peer(self, fs_name, token):
        self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_bootstrap",
                          "import", fs_name, token)

    def add_directory(self, fs_name, fs_id, dir_name, check_perf_counter=True):
        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.run_ceph_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count > dir_count)

        if check_perf_counter:
            res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
            vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
            self.assertGreater(vafter["counters"]["directory_count"], vbefore["counters"]["directory_count"])

    def remove_directory(self, fs_name, fs_id, dir_name):
        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vbefore = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.run_ceph_cmd("fs", "snapshot", "mirror", "remove", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count < dir_count)

        res = self.mirror_daemon_command(f'counter dump for fs: {fs_name}', 'counter', 'dump')
        vafter = res[TestMirroring.PERF_COUNTER_KEY_NAME_CEPHFS_MIRROR_FS][0]

        self.assertLess(vafter["counters"]["directory_count"], vbefore["counters"]["directory_count"])

    @retry_assert(timeout=140, interval=2)
    def check_mirror_status_after_failure(self):
        status = self.get_mirror_daemon_status()
        fs = status['filesystems'][0]
        peer = fs['peers'][0]

        self.assertEqual(fs['directory_count'], 1)
        self.assertEqual(peer['stats']['failure_count'], 1)
        self.assertEqual(peer['stats']['recovery_count'], 0)

    @retry_assert(timeout=140, interval=2)
    def check_mirror_status_after_failure_recovery(self):
        status = self.get_mirror_daemon_status()
        fs = status['filesystems'][0]
        peer = fs['peers'][0]

        self.assertEqual(fs['directory_count'], 1)
        self.assertEqual(peer['stats']['failure_count'], 1)
        self.assertEqual(peer['stats']['recovery_count'], 1)

    def peer_dir_status(self, res, dir_name, peer_uuid):
        self.assertIn('metrics', res)
        return res['metrics'][dir_name]['peer'][peer_uuid]

    def assert_last_synced_snap_metrics(self, last_synced_snap):
        for key in ('crawl_duration', 'datasync_queue_wait_duration', 'sync_duration',
                    'sync_time_stamp', 'sync_bytes', 'sync_files'):
            self.assertIn(key, last_synced_snap, msg=f'missing last_synced_snap.{key}')
        self.assertRegex(
            last_synced_snap['sync_bytes'],
            r'^\d+(\.\d+)?\s+(B|KiB|MiB|GiB|TiB|PiB)$')
        self.assertIsInstance(last_synced_snap['sync_files'], int)
        self.assertGreaterEqual(last_synced_snap['sync_files'], 0)

    def assert_syncing_snap_metrics(self, snap, sync_mode=None):
        for key in ('sync-mode', 'avg_read_throughput_bytes', 'avg_write_throughput_bytes',
                    'crawl', 'bytes', 'files', 'eta'):
            self.assertIn(key, snap, msg=f'missing current_syncing_snap.{key}')
        if sync_mode is not None:
            self.assertEqual(snap['sync-mode'], sync_mode)
        self.assertTrue(snap['avg_read_throughput_bytes'].endswith('/s'))
        self.assertTrue(snap['avg_write_throughput_bytes'].endswith('/s'))
        self.assertIn(snap['crawl']['state'], ('in-progress', 'completed'))
        self.assertTrue(snap['crawl']['duration'])
        bytes_obj = snap['bytes']
        self.assertIn('sync_bytes', bytes_obj)
        self.assertIn('total_bytes', bytes_obj)
        if bytes_obj.get('total_bytes') and bytes_obj['total_bytes'] != '0.00 B':
            self.assertIn('sync_percent', bytes_obj)
        files_obj = snap['files']
        self.assertIn('sync_files', files_obj)
        self.assertIn('total_files', files_obj)
        if files_obj.get('total_files', 0) > 0:
            self.assertIn('sync_percent', files_obj)
        self.assertTrue(
            snap['eta'] == 'calculating...' or bool(re.search(r'\d', snap['eta'])))

    def mgr_mirror_status(self, fs_name, mirrored_dir_path=None, peer_uuid=None):
        args = ["fs", "snapshot", "mirror", "status", fs_name]
        if mirrored_dir_path is not None:
            args.append(mirrored_dir_path)
        if peer_uuid is not None:
            args.append(peer_uuid)
        return json.loads(self.get_ceph_cmd_stdout(*args))

    def peer_status(self, fs_name, fs_id, peer_uuid):
        return self.mirror_daemon_command(
            f'peer status for fs: {fs_name}',
            'fs', 'mirror', 'peer', 'status',
            f'{fs_name}@{fs_id}', peer_uuid)

    def dir_status_from_mgr(self, fs_name, dir_name, peer_uuid,
                            mirrored_dir_path=None):
        res = self.mgr_mirror_status(
            fs_name, mirrored_dir_path or dir_name, peer_uuid)
        return self.peer_dir_status(res, dir_name, peer_uuid)

    def dir_status_from_asok(self, fs_name, fs_id, dir_name, peer_uuid):
        res = self.peer_status(fs_name, fs_id, peer_uuid)
        return self.peer_dir_status(res, dir_name, peer_uuid)

    def assert_default_idle_dir_stat(self, dir_stat):
        self.assertEqual(dir_stat['state'], 'idle')
        self.assertEqual(dir_stat['snaps_synced'], 0)
        self.assertEqual(dir_stat['snaps_deleted'], 0)
        self.assertEqual(dir_stat['snaps_renamed'], 0)
        self.assertNotIn('last_synced_snap', dir_stat)

    def assert_mgr_dir_stat_matches_asok(self, mgr_stat, asok_stat):
        self.assertEqual(mgr_stat['state'], asok_stat['state'])
        for key in ('snaps_synced', 'snaps_deleted', 'snaps_renamed'):
            self.assertEqual(mgr_stat.get(key), asok_stat.get(key))
        if 'failure_reason' in asok_stat:
            self.assertEqual(mgr_stat.get('failure_reason'), asok_stat['failure_reason'])
        if 'last_synced_snap' in asok_stat:
            self.assertIn('last_synced_snap', mgr_stat)
            self.assertEqual(mgr_stat['last_synced_snap']['name'],
                             asok_stat['last_synced_snap']['name'])
            self.assert_last_synced_snap_metrics(mgr_stat['last_synced_snap'])
        if 'current_syncing_snap' in asok_stat:
            self.assertIn('current_syncing_snap', mgr_stat)
            mgr_snap = mgr_stat['current_syncing_snap']
            asok_snap = asok_stat['current_syncing_snap']
            self.assertEqual(mgr_snap['name'], asok_snap['name'])
            self.assert_syncing_snap_metrics(
                mgr_snap, sync_mode=asok_snap.get('sync-mode'))

    @retry_assert(timeout=120, interval=2)
    def check_mgr_dir_stat_matches_asok(self, fs_name, fs_id, dir_name, peer_uuid,
                                      mirrored_dir_path=None):
        mgr_stat = self.dir_status_from_mgr(
            fs_name, dir_name, peer_uuid, mirrored_dir_path)
        asok_stat = self.dir_status_from_asok(fs_name, fs_id, dir_name, peer_uuid)
        try:
            self.assert_mgr_dir_stat_matches_asok(mgr_stat, asok_stat)
        except RETRY_EXCEPTIONS as e:
            e.mgr_stat = mgr_stat
            e.asok_stat = asok_stat
            raise

    def wait_for_mirror_daemon_stop(self, pid):
        with safe_while(sleep=1, tries=60,
                        action='wait for mirror daemon stop') as proceed:
            while proceed():
                try:
                    cur_pid = self.get_mirror_daemon_pid()
                except CommandFailedError:
                    return
                if cur_pid != pid:
                    return
                p = self.mount_a.run_shell(['kill', '-0', cur_pid],
                                           check_status=False)
                if p.returncode != 0:
                    return

    def restart_mirror_daemon(self):
        # daemon.start() always calls restart(), which skips stop() once proc
        # is cleared.  Stop the real cephfs-mirror via its pid file first so
        # the new instance can take the pidfile lock.
        daemons = list(self.ctx.daemons.iter_daemons_of_role('cephfs-mirror'))
        self.assertEqual(len(daemons), 1,
                         'expected a single cephfs-mirror daemon')
        daemon = daemons[0]
        rados_inst_before = self.get_mirror_rados_addr(
            self.primary_fs_name, self.primary_fs_id)
        pid = self.get_mirror_daemon_pid()

        log.debug(f'SIGTERM to cephfs-mirror pid {pid}')
        if daemon.running():
            try:
                daemon.signal(signal.SIGTERM, silent=True)
            except Exception as e:
                log.debug(f'failed to signal cephfs-mirror via teuthology: {e}')
        self.mount_a.run_shell(['kill', '-TERM', pid])
        self.wait_for_mirror_daemon_stop(pid)

        if daemon.running():
            try:
                run.wait([daemon.proc], timeout=10)
            except CommandFailedError:
                pass
        daemon.reset()

        log.debug('starting cephfs-mirror')
        daemon.start()

        with safe_while(sleep=2, tries=30,
                        action='wait for mirror daemon restart') as proceed:
            while proceed():
                try:
                    rados_inst = self.get_mirror_rados_addr(
                        self.primary_fs_name, self.primary_fs_id)
                    if rados_inst and rados_inst != rados_inst_before:
                        break
                except CommandFailedError:
                    pass

    def wait_for_mirror_daemon_recovery(self, fs_name, fs_id, dir_name, peer_uuid):
        # A new rados_inst alone does not mean mirroring is ready: wait until the
        # peer is configured, the snap dir is registered, and asok reports it.
        with safe_while(sleep=2, tries=60,
                        action='wait for mirror daemon recovery') as proceed:
            while proceed():
                if not self.get_mirror_rados_addr(fs_name, fs_id):
                    continue
                try:
                    mirror_res = self.mirror_daemon_command(
                        f'mirror status for fs: {fs_name}',
                        'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
                except CommandFailedError:
                    continue
                if peer_uuid not in mirror_res.get('peers', {}):
                    continue
                if mirror_res.get('snap_dirs', {}).get('dir_count', 0) < 1:
                    continue
                try:
                    peer_res = self.peer_status(fs_name, fs_id, peer_uuid)
                    self.peer_dir_status(peer_res, dir_name, peer_uuid)
                except RETRY_EXCEPTIONS:
                    continue
                return

    @retry_assert(timeout=120, interval=1)
    def check_mgr_and_asok_session_counters_zero(self, fs_name, fs_id, dir_name,
                                                 peer_uuid):
        mgr_stat = self.dir_status_from_mgr(fs_name, dir_name, peer_uuid)
        asok_stat = self.dir_status_from_asok(fs_name, fs_id, dir_name, peer_uuid)
        self.assertEqual(mgr_stat['state'], 'idle')
        self.assertEqual(asok_stat['state'], 'idle')
        for key in ('snaps_synced', 'snaps_deleted', 'snaps_renamed'):
            self.assertEqual(mgr_stat.get(key), 0, msg=f'mgr {key}')
            self.assertEqual(asok_stat.get(key), 0, msg=f'asok {key}')
        self.assertEqual(mgr_stat['last_synced_snap']['name'],
                         asok_stat['last_synced_snap']['name'])

    @retry_assert(timeout=90, interval=5)
    def check_mgr_dir_stat_stale(self, fs_name, dir_name, peer_uuid):
        mgr_stat = self.dir_status_from_mgr(fs_name, dir_name, peer_uuid)
        self.assertEqual(mgr_stat['state'], 'stale',
                         msg=f'unexpected mgr stat: {mgr_stat}')
        self.assertNotIn('current_syncing_snap', mgr_stat)

    @retry_assert(timeout=120, interval=1)
    def check_peer_syncing_progress_metrics(self, fs_name, fs_id, peer_spec, dir_name,
                                            snap_name, sync_mode=None):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertEqual(dir_stat['state'], 'syncing')
            snap = dir_stat['current_syncing_snap']
            self.assertEqual(snap['name'], snap_name)
            self.assert_syncing_snap_metrics(snap, sync_mode=sync_mode)
            if 'datasync_queue_wait' in snap:
                self.assertIn(snap['datasync_queue_wait']['state'],
                              ('waiting', 'completed'))
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=3)
    def check_peer_status_empty(self, fs_name, fs_id, peer_spec):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            self.assertFalse(res.get('metrics'))
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=600, interval=10)
    def check_peer_status(self, fs_name, fs_id, peer_spec, dir_name, expected_snap_name,
                          expected_snap_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['last_synced_snap']['name'] == expected_snap_name)
            self.assertTrue(dir_stat['snaps_synced'] == expected_snap_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=5)
    def check_peer_status_idle(self, fs_name, fs_id, peer_spec, dir_name, expected_snap_name,
                               expected_snap_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue('idle' == dir_stat['state'])
            self.assertTrue(expected_snap_name == dir_stat['last_synced_snap']['name'])
            self.assertTrue(expected_snap_count == dir_stat['snaps_synced'])
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=2)
    def check_peer_status_deleted_snap(self, fs_name, fs_id, peer_spec, dir_name,
                                      expected_delete_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['snaps_deleted'] == expected_delete_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=2)
    def check_peer_status_renamed_snap(self, fs_name, fs_id, peer_spec, dir_name,
                                       expected_rename_count):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        try:
            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue(dir_stat['snaps_renamed'] == expected_rename_count)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    @retry_assert(timeout=60, interval=1)
    def check_peer_snap_in_progress(self, fs_name, fs_id,
                                    peer_spec, dir_name, snap_name, timeout=60, interval=1):
        peer_uuid = self.get_peer_uuid(peer_spec)
        try:
            res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                             'fs', 'mirror', 'peer', 'status',
                                             f'{fs_name}@{fs_id}', peer_uuid)

            dir_stat = self.peer_dir_status(res, dir_name, peer_uuid)
            self.assertTrue('syncing' == dir_stat['state'])
            self.assertTrue(dir_stat['current_syncing_snap']['name'] == snap_name)
        except RETRY_EXCEPTIONS as e:
            e.res = res
            raise

    def verify_snapshot(self, dir_name, snap_name):
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertTrue(snap_name in snap_list)

        source_res = self.mount_a.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                               follow_symlinks=True)
        log.debug(f'source snapshot checksum {snap_name} {source_res}')

        dest_res = self.mount_b.dir_checksum(path=f'{dir_name}/.snap/{snap_name}',
                                             follow_symlinks=True)
        log.debug(f'destination snapshot checksum {snap_name} {dest_res}')
        self.assertTrue(source_res == dest_res)

    @retry_assert(timeout=150, interval=5)
    def verify_failed_directory(self, fs_name, fs_id, peer_spec, dir_name):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        self.assertTrue('failed' == self.peer_dir_status(res, dir_name, peer_uuid)['state'])

    def get_peer_uuid(self, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(self.primary_fs_name)
        peers = fs_map['mirror_info']['peers']
        for peer_uuid, mirror_info in peers.items():
            client_name = mirror_info['remote']['client_name']
            cluster_name = mirror_info['remote']['cluster_name']
            remote_peer_spec = f'{client_name}@{cluster_name}'
            if peer_spec == remote_peer_spec:
                return peer_uuid
        return None

    def get_daemon_admin_socket(self):
        """overloaded by teuthology override (fs/mirror/clients/mirror.yaml)"""
        return "/var/run/ceph/cephfs-mirror.asok"

    def get_mirror_daemon_pid(self):
        """pid file overloaded in fs/mirror/clients/mirror.yaml"""
        return self.mount_a.run_shell(['cat', '/var/run/ceph/cephfs-mirror.pid']).stdout.getvalue().strip()

    def get_mirror_rados_addr(self, fs_name, fs_id):
        """return the rados addr used by cephfs-mirror instance"""
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        if 'rados_inst' in res:
            return res['rados_inst']

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            # use mount_a's remote to execute command
            p = self.mount_a.client_remote.run(args=
                     ['ceph', '--admin-daemon', asok_path] + list(args),
                     stdout=StringIO(), stderr=StringIO(), timeout=30,
                     check_status=True, label=cmd_label)
            p.wait()
        except CommandFailedError as ce:
            log.warn(f'mirror daemon command with label "{cmd_label}" failed: {ce}')
            raise
        res = p.stdout.getvalue().strip()
        log.debug(f'command returned={res}')
        return json.loads(res)

    def get_mirror_daemon_status(self):
        daemon_status = json.loads(self.get_ceph_cmd_stdout("fs", "snapshot", "mirror", "daemon", "status"))
        log.debug(f'daemon_status: {daemon_status}')
        # running a single mirror daemon is supported
        status = daemon_status[0]
        log.debug(f'status: {status}')
        return status

    def setup_mount_b(self, mds_perm):
        log.debug('reconfigure client auth caps')
        self.get_ceph_cmd_result(
            'auth', 'caps', f"client.{self.mount_b.client_id}",
            'mds', f'allow {mds_perm}',
            'mon', 'allow r',
            'osd', f"allow rw pool={self.backup_fs.get_data_pool_name()}")
        self.mount_b.umount_wait()
        log.debug(f'mounting filesystem {self.secondary_fs_name}')
        self.mount_b.mount_wait(cephfs_name=self.secondary_fs_name)

    def test_mirror_peer_commands(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)
        # remove peer
        self.peer_remove(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mirror_disable_with_peer(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mirror_peer_add_existing(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        # adding the same peer should be idempotent
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name, check_perf_counter=False)

        # remove peer
        self.peer_remove(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_peer_commands_with_mirroring_disabled(self):
        # try adding peer when mirroring is not enabled
        try:
            self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name, check_perf_counter=False)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_add to fail')

        # try removing peer
        try:
            self.run_ceph_cmd("fs", "snapshot", "mirror", "peer_remove", self.primary_fs_name, 'dummy-uuid')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when removing a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_remove to fail')

    def test_mirroring_init_failure(self):
        """Test mirror daemon init failure"""

        # disable mgr mirroring plugin as it would try to load dir map on
        # on mirroring enabled for a filesystem (an throw up errors in
        # the logs)
        self.disable_mirroring_module()

        # enable mirroring through mon interface -- this should result in the mirror daemon
        # failing to enable mirroring due to absence of `cephfs_mirror` index object.
        self.run_ceph_cmd("fs", "mirror", "enable", self.primary_fs_name)

        with safe_while(sleep=5, tries=10, action='wait for failed state') as proceed:
            while proceed():
                try:
                    # verify via asok
                    res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                                     'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
                    if not 'state' in res:
                        return
                    self.assertTrue(res['state'] == "failed")
                    return True
                except:
                    pass

        self.run_ceph_cmd("fs", "mirror", "disable", self.primary_fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                       'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

    def test_mirroring_init_failure_with_recovery(self):
        """Test if the mirror daemon can recover from a init failure"""

        # disable mgr mirroring plugin as it would try to load dir map on
        # on mirroring enabled for a filesystem (an throw up errors in
        # the logs)
        self.disable_mirroring_module()

        # enable mirroring through mon interface -- this should result in the mirror daemon
        # failing to enable mirroring due to absence of `cephfs_mirror` index object.

        self.run_ceph_cmd("fs", "mirror", "enable", self.primary_fs_name)
        # need safe_while since non-failed status pops up as mirroring is restarted
        # internally in mirror daemon.
        with safe_while(sleep=5, tries=20, action='wait for failed state') as proceed:
            while proceed():
                try:
                    # verify via asok
                    res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                                     'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
                    if not 'state' in res:
                        return
                    self.assertTrue(res['state'] == "failed")
                    return True
                except:
                    pass

        # create the index object and check daemon recovery
        try:
            p = self.mount_a.client_remote.run(args=['rados', '-p', self.fs.metadata_pool_name, 'create', 'cephfs_mirror'],
                                               stdout=StringIO(), stderr=StringIO(), timeout=30,
                                               check_status=True, label="create index object")
            p.wait()
        except CommandFailedError as ce:
            log.warn(f'mirror daemon command to create mirror index object failed: {ce}')
            raise
        time.sleep(30)
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

        self.run_ceph_cmd("fs", "mirror", "disable", self.primary_fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                       'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

    def test_mgr_snapshot_mirror_status_stale_after_daemon_stop(self):
        """Mgr status marks frozen omap syncing progress as stale."""
        self.setup_mount_b(mds_perm='rw')
        self.mount_a.run_shell(["mkdir", "d0"])
        for i in range(8):
            self.mount_a.write_n_mb(os.path.join('d0', f'file.{i}'), 1024)

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        peer_spec = "client.mirror_remote@ceph"
        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                      self.secondary_fs_name)

        self.mount_a.run_shell(["mkdir", "d0/.snap/snap0"])
        self.check_peer_snap_in_progress(self.primary_fs_name, self.primary_fs_id,
                                         peer_spec, '/d0', 'snap0')

        peer_uuid = self.get_peer_uuid(peer_spec)
        # Wait for live metrics to reach omap before freezing the daemon.
        # Stale detection requires a persisted _instance_id; without an omap
        # write the mgr reports default idle metrics instead of stale.
        with safe_while(sleep=2, tries=30,
                        action='wait for omap syncing metrics') as proceed:
            while proceed():
                mgr_stat = self.dir_status_from_mgr(
                    self.primary_fs_name, '/d0', peer_uuid)
                if mgr_stat.get('state') == 'syncing':
                    break

        pid = self.get_mirror_daemon_pid()
        log.debug(f'SIGSTOP to cephfs-mirror pid {pid}')
        self.mount_a.run_shell(['kill', '-SIGSTOP', pid])
        try:
            # InstanceWatcher INSTANCE_TIMEOUT is 30s; allow extra time for
            # the mgr notify loop to age out the frozen instance.
            time.sleep(40)
            self.check_mgr_dir_stat_stale(
                self.primary_fs_name, '/d0', peer_uuid)
        finally:
            log.debug('SIGCONT to cephfs-mirror')
            self.mount_a.run_shell(['kill', '-SIGCONT', pid])

        # wait for restart mirror on blocklist
        time.sleep(60)
        with safe_while(sleep=2, tries=20,
                        action='wait for mirror daemon recovery after SIGSTOP') as proceed:
            while proceed():
                if not self.get_mirror_rados_addr(self.primary_fs_name,
                                                   self.primary_fs_id):
                    continue
                res = self.mirror_daemon_command(
                    f'mirror status for fs: {self.primary_fs_name}',
                    'fs', 'mirror', 'status',
                    f'{self.primary_fs_name}@{self.primary_fs_id}')
                if 'snap_dirs' in res:
                    break

        self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mgr_snapshot_mirror_status_survives_daemon_restart(self):
        """Mgr status keeps persisted last_synced_snap and resets session counters after restart."""
        self.setup_mount_b(mds_perm='rw')
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        peer_spec = "client.mirror_remote@ceph"
        self.peer_add(self.primary_fs_name, self.primary_fs_id, peer_spec,
                      self.secondary_fs_name)

        dir_name = 'mgr_restart_dir'
        self.mount_a.run_shell(['mkdir', dir_name])
        self.mount_a.create_n_files(f'{dir_name}/file', 3000, sync=True)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, f'/{dir_name}')

        snap0 = 'snap0'
        self.mount_a.run_shell(['mkdir', f'{dir_name}/.snap/{snap0}'])
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap0, 1)

        for i in range(5):
            self.mount_a.write_n_mb(os.path.join(dir_name, f'more_file.{i}'), 1)

        snap1 = 'snap1'
        self.mount_a.run_shell(['mkdir', f'{dir_name}/.snap/{snap1}'])
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap1, 2)

        self.mount_a.run_shell(['rmdir', f'{dir_name}/.snap/{snap0}'])
        self.check_peer_status_deleted_snap(self.primary_fs_name, self.primary_fs_id,
                                            peer_spec, f'/{dir_name}', 1)
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertNotIn(snap0, snap_list)

        snap2 = 'snap2'
        self.mount_a.run_shell(['mv', f'{dir_name}/.snap/{snap1}',
                                f'{dir_name}/.snap/{snap2}'])
        self.check_peer_status_renamed_snap(self.primary_fs_name, self.primary_fs_id,
                                            peer_spec, f'/{dir_name}', 1)
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertNotIn(snap1, snap_list)
        self.assertIn(snap2, snap_list)
        self.check_peer_status_idle(self.primary_fs_name, self.primary_fs_id,
                                    peer_spec, f'/{dir_name}', snap2, 2)

        peer_uuid = self.get_peer_uuid(peer_spec)
        before = self.dir_status_from_mgr(
            self.primary_fs_name, f'/{dir_name}', peer_uuid)
        self.assertEqual(before['snaps_synced'], 2)
        self.assertEqual(before['snaps_deleted'], 1)
        self.assertEqual(before['snaps_renamed'], 1)
        self.assertEqual(before['last_synced_snap']['name'], snap2)

        self.restart_mirror_daemon()
        self.wait_for_mirror_daemon_recovery(
            self.primary_fs_name, self.primary_fs_id, f'/{dir_name}', peer_uuid)
        self.check_mgr_and_asok_session_counters_zero(
            self.primary_fs_name, self.primary_fs_id, f'/{dir_name}', peer_uuid)

        after = self.dir_status_from_mgr(
            self.primary_fs_name, f'/{dir_name}', peer_uuid)
        self.assertEqual(after['last_synced_snap']['name'],
                         before['last_synced_snap']['name'])
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

