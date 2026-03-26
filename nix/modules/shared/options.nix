{self}: {
  lib,
  pkgs,
  ...
}: {
  options.services.mithril = {
    enable = lib.mkEnableOption "Mithril full node";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.mithril;
      description = "Mithril package to run.";
    };

    configFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Path to Mithril config.toml (defaults to $CONFIGURATION_DIRECTORY/config.toml when unset).";
    };

    storage = {
      dataDir = lib.mkOption {
        type = lib.types.str;
        default = "/var/mithril";
        description = "Base directory for Mithril data when using external storage.";
      };

      singleDisk = {
        enable = lib.mkOption {
          type = lib.types.bool;
          default = false;
          description = "Use a single disk for all Mithril data (mounted once, with subdirectories).";
        };

        device = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Block device for single-disk setup (e.g. /dev/disk/by-id/... or /dev/nvme0n1).";
        };

        fsType = lib.mkOption {
          type = lib.types.enum ["ext4" "xfs" "f2fs"];
          default = "ext4";
          description = "Filesystem type for single-disk setup.";
        };

        mountPoint = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Mount point for single-disk setup.";
        };

        mountOptions = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = ["noatime" "nofail"];
          description = "Mount options for single-disk setup.";
        };

        format = {
          enable = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Format single disk if it has no filesystem.";
          };

          force = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Force reformat even if a filesystem exists (DESTRUCTIVE).";
          };
          label = lib.mkOption {
            type = lib.types.str;
            default = "mithril-data";
            description = "Filesystem label for single-disk setup.";
          };
        };
      };

      accounts = {
        device = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Block device for AccountsDB (e.g. /dev/disk/by-id/... or /dev/nvme0n1).";
        };

        fsType = lib.mkOption {
          type = lib.types.enum ["ext4" "xfs" "f2fs"];
          default = "ext4";
          description = "Filesystem type for AccountsDB disk.";
        };

        mountPoint = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Mount point for AccountsDB disk.";
        };

        mountOptions = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = ["noatime" "nofail"];
          description = "Mount options for AccountsDB disk.";
        };

        format = {
          enable = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Format AccountsDB disk if it has no filesystem.";
          };

          force = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Force reformat even if a filesystem exists (DESTRUCTIVE).";
          };
          label = lib.mkOption {
            type = lib.types.str;
            default = "mithril-accounts";
            description = "Filesystem label for AccountsDB disk.";
          };
        };
      };

      blocks = {
        device = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Block device for blockstore/snapshots (e.g. /dev/disk/by-id/... or /dev/nvme0n1).";
        };

        fsType = lib.mkOption {
          type = lib.types.enum ["ext4" "xfs" "f2fs"];
          default = "xfs";
          description = "Filesystem type for blocks disk.";
        };

        mountPoint = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Mount point for blockstore/snapshots disk.";
        };

        mountOptions = lib.mkOption {
          type = lib.types.listOf lib.types.str;
          default = ["noatime" "nofail"];
          description = "Mount options for blocks disk.";
        };

        format = {
          enable = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Format blocks disk if it has no filesystem.";
          };

          force = lib.mkOption {
            type = lib.types.bool;
            default = false;
            description = "Force reformat even if a filesystem exists (DESTRUCTIVE).";
          };
          label = lib.mkOption {
            type = lib.types.str;
            default = "mithril-blocks";
            description = "Filesystem label for blocks disk.";
          };
        };
      };
    };

    config = {
      generate = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Generate config.toml from Nix settings.";
      };

      settings = lib.mkOption {
        type = lib.types.attrs;
        default = {};
        description = "Config settings merged into the generated config.toml.";
      };
    };

    configSchema = {
      name = lib.mkOption {
        type = lib.types.str;
        default = "mithril";
        description = "Instance name (logs/metrics).";
      };

      bootstrapMode = lib.mkOption {
        type = lib.types.enum ["auto" "snapshot" "new-snapshot" "accountsdb"];
        default = "auto";
        description = "Bootstrap mode.";
      };

      storageAccounts = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "AccountsDB path override.";
      };

      storageBlockstore = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Blockstore path override.";
      };

      storageSnapshots = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Snapshots path override.";
      };

      storageLogs = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Logs path override.";
      };

      networkCluster = lib.mkOption {
        type = lib.types.enum ["mainnet-beta" "testnet" "devnet"];
        default = "mainnet-beta";
        description = "Solana cluster.";
      };

      networkRpc = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = ["https://api.mainnet-beta.solana.com"];
        description = "RPC endpoints in priority order.";
      };

      blockSource = lib.mkOption {
        type = lib.types.enum ["rpc" "lightbringer"];
        default = "rpc";
        description = "Block source.";
      };

      blockLightbringerEndpoint = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Lightbringer endpoint (when block source is lightbringer).";
      };

      blockMaxRps = lib.mkOption {
        type = lib.types.int;
        default = 8;
        description = "Max RPC requests per second for block fetching.";
      };

      blockMaxInflight = lib.mkOption {
        type = lib.types.int;
        default = 8;
        description = "Max concurrent block fetch requests.";
      };

      blockTipPollIntervalMs = lib.mkOption {
        type = lib.types.int;
        default = 1000;
        description = "Tip poll interval in catchup mode (ms).";
      };

      blockTipSafetyMargin = lib.mkOption {
        type = lib.types.int;
        default = 32;
        description = "Safety margin in slots when near tip.";
      };

      blockNearTipThreshold = lib.mkOption {
        type = lib.types.int;
        default = 32;
        description = "Enter near-tip mode when gap <= this.";
      };

      blockCatchupThreshold = lib.mkOption {
        type = lib.types.int;
        default = 64;
        description = "Exit near-tip mode when gap >= this.";
      };

      blockCatchupTipGateThreshold = lib.mkOption {
        type = lib.types.int;
        default = 128;
        description = "Apply tip safety margin only when gap > this.";
      };

      blockNearTipPollIntervalMs = lib.mkOption {
        type = lib.types.int;
        default = 500;
        description = "Tip poll interval in near-tip mode (ms).";
      };

      blockNearTipLookahead = lib.mkOption {
        type = lib.types.int;
        default = 2;
        description = "Lookahead slots in near-tip mode.";
      };

      replayTxpar = lib.mkOption {
        type = lib.types.int;
        default = 24;
        description = "Replay transaction parallelism.";
      };

      replayNumSlots = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = "Finite replay: number of slots.";
      };

      replayEndSlot = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = "Finite replay: end slot.";
      };

      rpcPort = lib.mkOption {
        type = lib.types.int;
        default = 8899;
        description = "RPC server port (0 disables).";
      };

      tuningZstdDecoderConcurrency = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = "Zstd decoder concurrency.";
      };

      tuningMaxConcurrentFlushers = lib.mkOption {
        type = lib.types.int;
        default = 16;
        description = "Max concurrent flushers.";
      };

      tuningParamArenaSizeMb = lib.mkOption {
        type = lib.types.int;
        default = 512;
        description = "Serialized parameter arena size (MB).";
      };

      tuningBorrowedAccountArenaSize = lib.mkOption {
        type = lib.types.int;
        default = 1024;
        description = "Borrowed account arena size.";
      };

      tuningUsePool = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable pool allocator for slices.";
      };

      tuningStoreAccountsWorkers = lib.mkOption {
        type = lib.types.int;
        default = 128;
        description = "Store accounts workers.";
      };

      tuningPprofPort = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = null;
        description = "pprof port (-1 disables).";
      };

      tuningPprofCpuProfilePath = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "CPU profile output path.";
      };

      debugTransactionSignatures = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        description = "Transaction signatures for debug logging.";
      };

      debugAccountWrites = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        description = "Account pubkeys for debug logging.";
      };

      snapshotMaxFullSnapshots = lib.mkOption {
        type = lib.types.int;
        default = 1;
        description = "Maximum full snapshots to keep.";
      };

      snapshotVerbose = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Enable verbose snapshot output.";
      };

      snapshotFullThreshold = lib.mkOption {
        type = lib.types.int;
        default = 100000;
        description = "Max age for full snapshots (slots).";
      };

      snapshotIncrementalThreshold = lib.mkOption {
        type = lib.types.int;
        default = 1000;
        description = "Max age for incremental snapshots (slots).";
      };

      snapshotSafetyMarginSlots = lib.mkOption {
        type = lib.types.int;
        default = 5000;
        description = "Snapshot expiration safety margin (slots).";
      };

      snapshotStage1WarmKib = lib.mkOption {
        type = lib.types.int;
        default = 512;
        description = "Stage 1 warmup (KiB).";
      };

      snapshotStage1WindowKib = lib.mkOption {
        type = lib.types.int;
        default = 512;
        description = "Stage 1 window size (KiB).";
      };

      snapshotStage1Windows = lib.mkOption {
        type = lib.types.int;
        default = 4;
        description = "Stage 1 window count.";
      };

      snapshotStage1TimeoutMs = lib.mkOption {
        type = lib.types.int;
        default = 3000;
        description = "Stage 1 timeout (ms).";
      };

      snapshotStage1Concurrency = lib.mkOption {
        type = lib.types.int;
        default = 0;
        description = "Stage 1 concurrency (0 = auto).";
      };

      snapshotStage2TopK = lib.mkOption {
        type = lib.types.int;
        default = 8;
        description = "Stage 2 top-K candidates.";
      };

      snapshotStage2WarmSec = lib.mkOption {
        type = lib.types.int;
        default = 3;
        description = "Stage 2 warmup (sec).";
      };

      snapshotStage2MeasureSec = lib.mkOption {
        type = lib.types.int;
        default = 3;
        description = "Stage 2 measurement (sec).";
      };

      snapshotStage2MinRatio = lib.mkOption {
        type = lib.types.float;
        default = 0.6;
        description = "Stage 2 minimum speed ratio.";
      };

      snapshotStage2MinAbsMbs = lib.mkOption {
        type = lib.types.float;
        default = 0.0;
        description = "Stage 2 minimum absolute speed (MB/s).";
      };

      snapshotMaxRttMs = lib.mkOption {
        type = lib.types.int;
        default = 200;
        description = "Maximum RTT for snapshot nodes (ms, 0 disables).";
      };

      snapshotTcpTimeoutMs = lib.mkOption {
        type = lib.types.int;
        default = 1000;
        description = "TCP timeout for snapshot pre-check (ms).";
      };

      snapshotMinNodeVersion = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Minimum Solana version required.";
      };

      snapshotAllowedNodeVersions = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [];
        description = "Allowed Solana versions.";
      };

      snapshotWorkerCount = lib.mkOption {
        type = lib.types.int;
        default = 100;
        description = "Snapshot worker count.";
      };

      snapshotMaxSnapshotUrlAttempts = lib.mkOption {
        type = lib.types.int;
        default = 3;
        description = "Max ranked snapshot URL attempts.";
      };

      snapshotMinIncrementalSpeedMbs = lib.mkOption {
        type = lib.types.float;
        default = 2.0;
        description = "Minimum incremental snapshot speed (MB/s).";
      };

      logLevel = lib.mkOption {
        type = lib.types.enum ["debug" "info" "warn" "error"];
        default = "info";
        description = "Log level.";
      };

      logTarget = lib.mkOption {
        type = lib.types.enum ["file" "journald" "both"];
        default = "both";
        description = ''
          Where to send logs.
          - "file": write log files only (no stdout).
          - "journald": log to stdout only (captured by journald on NixOS, system log on macOS).
          - "both": log to both stdout and files.
        '';
      };

      logMaxSizeMb = lib.mkOption {
        type = lib.types.int;
        default = 100;
        description = "Max log file size (MB). Only applies when logTarget includes file logging.";
      };

      logMaxAgeDays = lib.mkOption {
        type = lib.types.int;
        default = 30;
        description = "Max log file age (days). Only applies when logTarget includes file logging.";
      };

      logMaxBackups = lib.mkOption {
        type = lib.types.int;
        default = 100;
        description = "Max log backups (0 = unlimited). Only applies when logTarget includes file logging.";
      };
    };

    darwin = {
      paths = {
        stateDir = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Override state directory on macOS (defaults to XDG state home).";
        };

        accountsPath = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Override AccountsDB path on macOS (defaults to <stateDir>/accounts).";
        };

        blocksRoot = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Override blocks root on macOS (defaults to <stateDir>/blocks).";
        };

        logsDir = lib.mkOption {
          type = lib.types.nullOr lib.types.str;
          default = null;
          description = "Override logs directory on macOS (defaults to configSchema.storageLogs when set).";
        };
      };
    };
    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open firewall ports for configured Mithril services.";
    };

    performance = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Apply performance tuning (sysctl, TRIM, CPU, I/O).";
      };

      enableTrim = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable fstrim.timer for SSD TRIM.";
      };

      cpuGovernor = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "CPU frequency governor to set.";
      };

      sysctl = lib.mkOption {
        type = lib.types.attrsOf lib.types.str;
        default = {
          "vm.swappiness" = "10";
          "vm.vfs_cache_pressure" = "50";
          "vm.max_map_count" = "1000000";
          "fs.file-max" = "2097152";
        };
        description = "Sysctl settings to apply.";
      };

      transparentHugePages = lib.mkOption {
        type = lib.types.enum ["always" "madvise" "never"];
        default = "madvise";
        description = "Transparent hugepages setting.";
      };

      ioScheduler = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = "none";
        description = "I/O scheduler for NVMe devices.";
      };

      readAheadKb = lib.mkOption {
        type = lib.types.nullOr lib.types.int;
        default = 64;
        description = "Disk read-ahead (KB).";
      };
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [];
      description = "Extra arguments appended to the mithril run command.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "mithril";
      description = "Dynamic user name for the Mithril service.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "mithril";
      description = "Dynamic group name for the Mithril service.";
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = {};
      description = "Environment variables for the Mithril service.";
    };

    environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "EnvironmentFile-style file to load for the Mithril service (systemd on Linux, launchd wrapper on macOS).";
    };
  };
}
