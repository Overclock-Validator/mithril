{
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.services.mithril;
  escapeSystemdPath = path: let
    stripped = lib.removePrefix "/" path;
    escaped = lib.replaceStrings ["-"] ["\\x2d"] stripped;
  in
    lib.replaceStrings ["/"] ["-"] escaped;
  dataDir = cfg.storage.dataDir;
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
  singleDeviceUnit =
    if cfg.storage.singleDisk.device != null
    then "${escapeSystemdPath cfg.storage.singleDisk.device}.device"
    else null;
  accountsDeviceUnit =
    if cfg.storage.accounts.device != null
    then "${escapeSystemdPath cfg.storage.accounts.device}.device"
    else null;
  blocksDeviceUnit =
    if cfg.storage.blocks.device != null
    then "${escapeSystemdPath cfg.storage.blocks.device}.device"
    else null;
in {
  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          !(
            cfg.storage.singleDisk.enable
            && (cfg.storage.accounts.device != null || cfg.storage.blocks.device != null)
          );
        message = "services.mithril.storage.singleDisk.enable cannot be used with storage.accounts.device or storage.blocks.device.";
      }
      {
        assertion = !(cfg.storage.singleDisk.enable && cfg.storage.singleDisk.device == null);
        message = "services.mithril.storage.singleDisk.enable requires storage.singleDisk.device.";
      }
      {
        assertion = !(cfg.storage.singleDisk.enable && cfg.storage.singleDisk.mountPoint == null);
        message = "services.mithril.storage.singleDisk.enable requires storage.singleDisk.mountPoint.";
      }
      {
        assertion = !(cfg.storage.accounts.device != null && cfg.storage.accounts.mountPoint == null);
        message = "services.mithril.storage.accounts.device requires storage.accounts.mountPoint.";
      }
      {
        assertion = !(cfg.storage.blocks.device != null && cfg.storage.blocks.mountPoint == null);
        message = "services.mithril.storage.blocks.device requires storage.blocks.mountPoint.";
      }
      {
        assertion = !lib.hasPrefix "/var/lib/mithril" cfg.storage.dataDir;
        message = "services.mithril.storage.dataDir must not be under /var/lib/mithril — it conflicts with the systemd StateDirectory used by DynamicUser.";
      }
    ];

    services.mithril.storage = {
      singleDisk.mountPoint = lib.mkIf cfg.storage.singleDisk.enable (
        lib.mkDefault dataDir
      );
      accounts.mountPoint = lib.mkDefault (
        if cfg.storage.singleDisk.enable
        then "${cfg.storage.singleDisk.mountPoint}/accounts"
        else "${dataDir}/accounts"
      );
      blocks.mountPoint = lib.mkDefault (
        if cfg.storage.singleDisk.enable
        then "${cfg.storage.singleDisk.mountPoint}/blocks"
        else "${dataDir}/blocks"
      );
    };

    systemd.tmpfiles.rules =
      lib.optional cfg.storage.singleDisk.enable "d ${cfg.storage.singleDisk.mountPoint} 0755 root root - -"
      ++ lib.optional (
        cfg.storage.singleDisk.enable || cfg.storage.accounts.device != null
      ) "d ${cfg.storage.accounts.mountPoint} 0755 root root - -"
      ++ lib.optional (
        cfg.storage.singleDisk.enable || cfg.storage.blocks.device != null
      ) "d ${cfg.storage.blocks.mountPoint} 0755 root root - -";

    fileSystems = lib.mkMerge [
      (lib.mkIf cfg.storage.singleDisk.enable {
        ${cfg.storage.singleDisk.mountPoint} = {
          inherit (cfg.storage.singleDisk) device;
          inherit (cfg.storage.singleDisk) fsType;
          options = cfg.storage.singleDisk.mountOptions;
        };
      })
      (lib.mkIf (cfg.storage.accounts.device != null) {
        ${cfg.storage.accounts.mountPoint} = {
          inherit (cfg.storage.accounts) device;
          inherit (cfg.storage.accounts) fsType;
          options = cfg.storage.accounts.mountOptions;
        };
      })
      (lib.mkIf (cfg.storage.blocks.device != null) {
        ${cfg.storage.blocks.mountPoint} = {
          inherit (cfg.storage.blocks) device;
          inherit (cfg.storage.blocks) fsType;
          options = cfg.storage.blocks.mountOptions;
        };
      })
    ];

    systemd.services = let
      # blkid exit codes: 0 = found, 2 = no filesystem, anything else = error.
      # The old script used `|| true` which swallowed all errors, so if blkid
      # failed (device not ready, permission denied, etc.) the script would
      # see an empty $existing and reformat the drive on every boot.
      mkFormatScript = {
        device,
        fsType,
        label,
        force,
        diskLabel,
      }: ''
        set -euo pipefail
        device="${device}"
        fstype="${fsType}"
        label="${label}"

        if [ ! -b "$device" ]; then
          echo "${diskLabel}: device $device not found, refusing to format" >&2
          exit 1
        fi

        rc=0
        existing="$(blkid -o value -s TYPE "$device")" || rc=$?
        if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then
          echo "${diskLabel}: blkid failed (exit $rc), refusing to format" >&2
          exit 1
        fi

        ${
          if force
          then ''
            if [ -n "$existing" ]; then
              echo "${diskLabel}: wiping existing $existing filesystem (force=true)"
              wipefs -a "$device"
            fi
          ''
          else ''
            if [ -n "$existing" ]; then
              echo "${diskLabel}: already formatted as $existing. Skipping."
              exit 0
            fi
          ''
        }

        echo "${diskLabel}: formatting $device as $fstype (label=$label)"
        case "$fstype" in
          ext4) mkfs.ext4 -F -L "$label" "$device" ;;
          xfs)  mkfs.xfs -f -L "$label" "$device" ;;
          f2fs) mkfs.f2fs -f -l "$label" "$device" ;;
        esac
      '';
      formatServiceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      formatPath = [
        pkgs.util-linux
        pkgs.e2fsprogs
        pkgs.xfsprogs
        pkgs.f2fs-tools
      ];
    in
      lib.mkMerge [
        (lib.mkIf (cfg.storage.singleDisk.enable && cfg.storage.singleDisk.format.enable) {
          mithril-format-single = {
            description = "Format Mithril single disk if needed";
            after = [singleDeviceUnit];
            requires = [singleDeviceUnit];
            before = [singleMountUnit];
            requiredBy = [singleMountUnit];
            serviceConfig = formatServiceConfig;
            path = formatPath;
            script = mkFormatScript {
              device = cfg.storage.singleDisk.device;
              fsType = cfg.storage.singleDisk.fsType;
              label = cfg.storage.singleDisk.format.label;
              force = cfg.storage.singleDisk.format.force;
              diskLabel = "single-disk";
            };
          };
        })
        (lib.mkIf (cfg.storage.accounts.device != null && cfg.storage.accounts.format.enable) {
          mithril-format-accounts = {
            description = "Format Mithril AccountsDB disk if needed";
            after = [accountsDeviceUnit];
            requires = [accountsDeviceUnit];
            before = [accountsMountUnit];
            requiredBy = [accountsMountUnit];
            serviceConfig = formatServiceConfig;
            path = formatPath;
            script = mkFormatScript {
              device = cfg.storage.accounts.device;
              fsType = cfg.storage.accounts.fsType;
              label = cfg.storage.accounts.format.label;
              force = cfg.storage.accounts.format.force;
              diskLabel = "accounts";
            };
          };
        })
        (lib.mkIf (cfg.storage.blocks.device != null && cfg.storage.blocks.format.enable) {
          mithril-format-blocks = {
            description = "Format Mithril blocks disk if needed";
            after = [blocksDeviceUnit];
            requires = [blocksDeviceUnit];
            before = [blocksMountUnit];
            requiredBy = [blocksMountUnit];
            serviceConfig = formatServiceConfig;
            path = formatPath;
            script = mkFormatScript {
              device = cfg.storage.blocks.device;
              fsType = cfg.storage.blocks.fsType;
              label = cfg.storage.blocks.format.label;
              force = cfg.storage.blocks.format.force;
              diskLabel = "blocks";
            };
          };
        })
      ];
  };
}
