{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  effectiveGenerate = cfg.config.generate || cfg.environmentFile != null;
  escapeSystemdPath = path: let
    stripped = lib.removePrefix "/" path;
    escaped = lib.replaceStrings ["-"] ["\\x2d"] stripped;
  in
    lib.replaceStrings ["/"] ["-"] escaped;
  accountsMountUnit =
    if cfg.storage.accounts.mountPoint != null
    then "${escapeSystemdPath cfg.storage.accounts.mountPoint}.mount"
    else null;
  blocksMountUnit =
    if cfg.storage.blocks.mountPoint != null
    then "${escapeSystemdPath cfg.storage.blocks.mountPoint}.mount"
    else null;
  singleMountUnit =
    if cfg.storage.singleDisk.mountPoint != null
    then "${escapeSystemdPath cfg.storage.singleDisk.mountPoint}.mount"
    else null;
  fileLoggingEnabled = cfg.configSchema.logTarget == "file" || cfg.configSchema.logTarget == "both";
  hasExternalStorage =
    cfg.storage.singleDisk.enable
    || cfg.storage.accounts.device != null
    || cfg.storage.blocks.device != null;
  shared = import ../shared/lib.nix {inherit lib pkgs;};
  configTemplate = shared.mkConfigTomlTemplate {
    inherit cfg;
    accountsPath =
      if cfg.storage.accounts.mountPoint != null
      then cfg.storage.accounts.mountPoint
      else "@STATE_DIRECTORY@/accounts";
    blocksRoot =
      if cfg.storage.blocks.mountPoint != null
      then cfg.storage.blocks.mountPoint
      else "@STATE_DIRECTORY@/blocks";
    logsPath =
      if fileLoggingEnabled
      then "@LOGS_DIRECTORY@"
      else "@STATE_DIRECTORY@/logs";
  };
  mkdirsScript = pkgs.writeShellScript "mithril-mkdirs" ''
    set -euo pipefail
    ${lib.optionalString hasExternalStorage ''
      # External storage mounts are root-owned after mkfs.
      # Chown mount point roots so the DynamicUser can write.
      if [ -n "${singleDiskMountPoint}" ]; then
        chown "${cfg.user}:${cfg.group}" "${singleDiskMountPoint}"
      fi
      ${lib.optionalString (cfg.storage.accounts.device != null) ''
        if [ -n "${accountsMountPoint}" ]; then
          chown "${cfg.user}:${cfg.group}" "${accountsMountPoint}"
        fi
      ''}
      ${lib.optionalString (cfg.storage.blocks.device != null) ''
        if [ -n "${blocksMountPoint}" ]; then
          chown "${cfg.user}:${cfg.group}" "${blocksMountPoint}"
        fi
      ''}
    ''}
    if [ -n "${accountsMountPoint}" ]; then
      install -d -m 0755 ${lib.optionalString hasExternalStorage "-o ${cfg.user} -g ${cfg.group}"} "${accountsMountPoint}"
    fi
    if [ -n "${blocksMountPoint}" ]; then
      install -d -m 0755 ${lib.optionalString hasExternalStorage "-o ${cfg.user} -g ${cfg.group}"} "${blocksMountPoint}"
    fi
    if [ -n "${logsMountPoint}" ]; then
      install -d -m 0755 ${lib.optionalString hasExternalStorage "-o ${cfg.user} -g ${cfg.group}"} "${logsMountPoint}"
    fi
  '';
  configInitScript = pkgs.writeShellScript "mithril-generate-config" ''
    set -euo pipefail
    config_dir="$CONFIGURATION_DIRECTORY"
    state_dir="$STATE_DIRECTORY"
    logs_dir="''${LOGS_DIRECTORY:-}"
    runtime_dir="$RUNTIME_DIRECTORY"
    mkdir -p "$config_dir"
    mkdir -p "$runtime_dir"
    cat > "$config_dir/config.toml" <<'EOF'
    ${configTemplate}
    EOF
    sed -i "s|@STATE_DIRECTORY@|$state_dir|g" "$config_dir/config.toml"
    sed -i "s|@LOGS_DIRECTORY@|$logs_dir|g" "$config_dir/config.toml"
    awk '
      function esc(s, t) {
        t = s
        gsub(/\\/,"\\\\", t)
        gsub(/&/,"\\\\&", t)
        return t
      }
      function bylen(i1, v1, i2, v2, l1, l2) {
        l1 = length(v1)
        l2 = length(v2)
        if (l1 == l2) {
          return (v1 < v2) ? -1 : (v1 > v2)
        }
        return (l1 > l2) ? -1 : 1
      }
      BEGIN {
        for (name in ENVIRON) {
          names[name] = name
        }
        n = asorti(names, sorted, "bylen")
      }
      {
        line = $0
        for (i = 1; i <= n; i++) {
          name = sorted[i]
          pattern = "\\$" name
          gsub(pattern, esc(ENVIRON[name]), line)
          pattern = "\\$\\{" name "\\}"
          gsub(pattern, esc(ENVIRON[name]), line)
        }
        print line
      }
    ' "$config_dir/config.toml" > "$runtime_dir/config.toml.tmp"
    mv "$runtime_dir/config.toml.tmp" "$config_dir/config.toml"
  '';
  singleDiskMountPoint =
    if cfg.storage.singleDisk.mountPoint != null
    then cfg.storage.singleDisk.mountPoint
    else "";
  accountsMountPoint =
    if cfg.storage.accounts.mountPoint != null
    then cfg.storage.accounts.mountPoint
    else "";
  blocksMountPoint =
    if cfg.storage.blocks.mountPoint != null
    then cfg.storage.blocks.mountPoint
    else "";
  logsMountPoint =
    if fileLoggingEnabled && cfg.configSchema.storageLogs != null
    then cfg.configSchema.storageLogs
    else "";
in {
  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = !(cfg.configFile != null && cfg.environmentFile != null);
        message = "services.mithril.configFile and services.mithril.environmentFile cannot both be set.";
      }
    ];
    systemd.services = {
      mithril = {
        path = [pkgs.coreutils pkgs.gnused pkgs.gawk];
        after =
          ["network-online.target"]
          ++ lib.optional (cfg.storage.singleDisk.enable && singleMountUnit != null) singleMountUnit
          ++ lib.optional (cfg.storage.accounts.device != null && accountsMountUnit != null) accountsMountUnit
          ++ lib.optional (cfg.storage.blocks.device != null && blocksMountUnit != null) blocksMountUnit;

        requires =
          lib.optional (cfg.storage.singleDisk.enable && singleMountUnit != null) singleMountUnit
          ++ lib.optional (cfg.storage.accounts.device != null && accountsMountUnit != null) accountsMountUnit
          ++ lib.optional (cfg.storage.blocks.device != null && blocksMountUnit != null) blocksMountUnit;

        wants =
          ["network-online.target"]
          ++ lib.optional (cfg.storage.singleDisk.enable && singleMountUnit != null) singleMountUnit
          ++ lib.optional (cfg.storage.accounts.device != null && accountsMountUnit != null) accountsMountUnit
          ++ lib.optional (cfg.storage.blocks.device != null && blocksMountUnit != null) blocksMountUnit;

        serviceConfig.ExecStartPre =
          ["+${mkdirsScript}"]
          ++ lib.optionals (effectiveGenerate && cfg.configFile == null) ["+${configInitScript}"];
      };

      mithril-thp = lib.mkIf cfg.performance.enable {
        description = "Configure transparent huge pages for Mithril";
        wantedBy = ["multi-user.target"];
        serviceConfig = {
          Type = "oneshot";
        };
        script = ''
          set -euo pipefail
          if [ -w /sys/kernel/mm/transparent_hugepage/enabled ]; then
            echo "${cfg.performance.transparentHugePages}" > /sys/kernel/mm/transparent_hugepage/enabled
          fi
          if [ -w /sys/kernel/mm/transparent_hugepage/defrag ]; then
            if [ "${cfg.performance.transparentHugePages}" = "never" ]; then
              echo "never" > /sys/kernel/mm/transparent_hugepage/defrag
            else
              echo "defer+madvise" > /sys/kernel/mm/transparent_hugepage/defrag
            fi
          fi
        '';
      };
    };

    boot.kernel.sysctl = lib.mkIf cfg.performance.enable cfg.performance.sysctl;

    services.fstrim.enable = lib.mkIf cfg.performance.enable cfg.performance.enableTrim;

    powerManagement.cpuFreqGovernor =
      lib.mkIf (
        cfg.performance.enable && cfg.performance.cpuGovernor != null
      )
      cfg.performance.cpuGovernor;

    services.udev.extraRules = lib.mkIf cfg.performance.enable (
      let
        ioRule = lib.optionalString (cfg.performance.ioScheduler != null) ''
          ACTION=="add|change", KERNEL=="nvme[0-9]n[0-9]", ATTR{queue/scheduler}="${cfg.performance.ioScheduler}"
        '';
        raRule = lib.optionalString (cfg.performance.readAheadKb != null) ''
          ACTION=="add|change", KERNEL=="nvme[0-9]n[0-9]", ATTR{bdi/read_ahead_kb}="${toString cfg.performance.readAheadKb}"
        '';
      in
        ioRule + raRule
    );

    networking.firewall = lib.mkIf cfg.openFirewall {
      allowedTCPPorts = lib.unique (
        lib.filter (p: p > 0) [
          cfg.configSchema.rpcPort
          (cfg.configSchema.tuningPprofPort or 0)
        ]
      );
    };
  };
}
