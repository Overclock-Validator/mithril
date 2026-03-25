{
  lib,
  pkgs,
}: let
  removeNulls = attrs: lib.filterAttrsRecursive (_: v: v != null) attrs;
  mkConfigToml = {
    cfg,
    accountsPath,
    blocksRoot,
    logsPath ? null,
  }: let
    storageAccounts =
      if cfg.configSchema.storageAccounts != null
      then cfg.configSchema.storageAccounts
      else accountsPath;
    storageBlockstore =
      if cfg.configSchema.storageBlockstore != null
      then cfg.configSchema.storageBlockstore
      else "${blocksRoot}/blockstore";
    storageSnapshots =
      if cfg.configSchema.storageSnapshots != null
      then cfg.configSchema.storageSnapshots
      else "${blocksRoot}/snapshots";
    fileLoggingEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
    storageLogs =
      if cfg.configSchema.storageLogs != null
      then cfg.configSchema.storageLogs
      else logsPath;
    baseConfig = {
      inherit (cfg.configSchema) name;
      bootstrap.mode = cfg.configSchema.bootstrapMode;
      storage = {
        accounts = storageAccounts;
        blockstore = storageBlockstore;
        snapshots = storageSnapshots;
        logs = storageLogs;
      };
      network = {
        cluster = cfg.configSchema.networkCluster;
        rpc = cfg.configSchema.networkRpc;
      };
      block = {
        source = cfg.configSchema.blockSource;
        lightbringer_endpoint = cfg.configSchema.blockLightbringerEndpoint;
        max_rps = cfg.configSchema.blockMaxRps;
        max_inflight = cfg.configSchema.blockMaxInflight;
        tip_poll_interval_ms = cfg.configSchema.blockTipPollIntervalMs;
        tip_safety_margin = cfg.configSchema.blockTipSafetyMargin;
        near_tip_threshold = cfg.configSchema.blockNearTipThreshold;
        catchup_threshold = cfg.configSchema.blockCatchupThreshold;
        catchup_tip_gate_threshold = cfg.configSchema.blockCatchupTipGateThreshold;
        near_tip_poll_interval_ms = cfg.configSchema.blockNearTipPollIntervalMs;
        near_tip_lookahead = cfg.configSchema.blockNearTipLookahead;
      };
      replay = {
        txpar = cfg.configSchema.replayTxpar;
        num_slots = cfg.configSchema.replayNumSlots;
        end_slot = cfg.configSchema.replayEndSlot;
      };
      rpc = {
        port = cfg.configSchema.rpcPort;
      };
      tuning = {
        zstd_decoder_concurrency = cfg.configSchema.tuningZstdDecoderConcurrency;
        max_concurrent_flushers = cfg.configSchema.tuningMaxConcurrentFlushers;
        param_arena_size_mb = cfg.configSchema.tuningParamArenaSizeMb;
        borrowed_account_arena_size = cfg.configSchema.tuningBorrowedAccountArenaSize;
        use_pool = cfg.configSchema.tuningUsePool;
        store_accounts_workers = cfg.configSchema.tuningStoreAccountsWorkers;
        pprof = {
          port = cfg.configSchema.tuningPprofPort;
          cpu_profile_path = cfg.configSchema.tuningPprofCpuProfilePath;
        };
      };
      debug = {
        transaction_signatures = cfg.configSchema.debugTransactionSignatures;
        account_writes = cfg.configSchema.debugAccountWrites;
      };
      snapshot = {
        max_full_snapshots = cfg.configSchema.snapshotMaxFullSnapshots;
        verbose = cfg.configSchema.snapshotVerbose;
        full_threshold = cfg.configSchema.snapshotFullThreshold;
        incremental_threshold = cfg.configSchema.snapshotIncrementalThreshold;
        safety_margin_slots = cfg.configSchema.snapshotSafetyMarginSlots;
        stage1_warm_kib = cfg.configSchema.snapshotStage1WarmKib;
        stage1_window_kib = cfg.configSchema.snapshotStage1WindowKib;
        stage1_windows = cfg.configSchema.snapshotStage1Windows;
        stage1_timeout_ms = cfg.configSchema.snapshotStage1TimeoutMs;
        stage1_concurrency = cfg.configSchema.snapshotStage1Concurrency;
        stage2_top_k = cfg.configSchema.snapshotStage2TopK;
        stage2_warm_sec = cfg.configSchema.snapshotStage2WarmSec;
        stage2_measure_sec = cfg.configSchema.snapshotStage2MeasureSec;
        stage2_min_ratio = cfg.configSchema.snapshotStage2MinRatio;
        stage2_min_abs_mbs = cfg.configSchema.snapshotStage2MinAbsMbs;
        max_rtt_ms = cfg.configSchema.snapshotMaxRttMs;
        tcp_timeout_ms = cfg.configSchema.snapshotTcpTimeoutMs;
        min_node_version = cfg.configSchema.snapshotMinNodeVersion;
        allowed_node_versions = cfg.configSchema.snapshotAllowedNodeVersions;
        worker_count = cfg.configSchema.snapshotWorkerCount;
        max_snapshot_url_attempts = cfg.configSchema.snapshotMaxSnapshotUrlAttempts;
        min_incremental_speed_mbs = cfg.configSchema.snapshotMinIncrementalSpeedMbs;
      };
      log = let
        fileEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
        stdoutEnabled = cfg.configSchema.logTarget == "journald" || cfg.configSchema.logTarget == "both";
      in {
        level = cfg.configSchema.logLevel;
        to_stdout = stdoutEnabled;
        max_size_mb =
          if fileEnabled
          then cfg.configSchema.logMaxSizeMb
          else null;
        max_age_days =
          if fileEnabled
          then cfg.configSchema.logMaxAgeDays
          else null;
        max_backups =
          if fileEnabled
          then cfg.configSchema.logMaxBackups
          else null;
      };
    };
    finalConfig = removeNulls (lib.recursiveUpdate baseConfig cfg.config.settings);
  in
    (pkgs.formats.toml {}).generate "mithril.toml" finalConfig;
  mkConfigTomlTemplate = {
    cfg,
    accountsPath,
    blocksRoot,
    logsPath ? null,
  }:
    builtins.readFile (mkConfigToml {inherit cfg accountsPath blocksRoot logsPath;});
in {
  inherit mkConfigToml mkConfigTomlTemplate;
}
