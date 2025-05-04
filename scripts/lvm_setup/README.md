# LVM Setup and Mithril Workflow with Snapshots

This document outlines the steps to set up LVM thin provisioning on a dedicated NVMe drive, build a Solana accounts database using Mithril, and then use LVM snapshots to run subsequent Mithril processes without modifying the original database.

**Assumes:**
* You have two bash scripts saved: `setup_mithril_lvm.sh` (for infrastructure) and `manage_mithril_snapshot.sh` (for snapshot operations).
* You have a Python 3 script `snapshot-finder.py` designed to find and download a suitable Solana snapshot `.tar.zst` file.
* You have the `mithril` binary (likely at `~/mithril/mithril`).
* You have necessary permissions (most commands require `sudo`).
* You have LVM tools (`lvm2`), filesystem tools (`e2fsprogs`), and other Linux utilities installed.

**IMPORTANT CONFIGURATION:** ⚠️
* **BEFORE STARTING:** Edit **both** `setup_mithril_lvm.sh` and `manage_mithril_snapshot.sh` to ensure the configuration variables match your environment, especially:
    * `DEVICE`: The target NVMe drive (e.g., `/dev/nvme1n1`). **Verify this carefully!**
    * `VG_NAME`: Volume group name (e.g., `accounts_vg_run`). Must match in both scripts.
    * `MASTER_LV_NAME`: Name for the original LV (e.g., `accounts_master`). Must match in both scripts.
    * `LVM_PCT`: Percentage of disk for LVM in `setup_mithril_lvm.sh`. This controls the size trade-off between the accounts DB partition (`/mnt/accounts_master`) and the partition for other data/downloads (`/var/lib/mithril`).
        > **Disk Space Warning:** You need enough space on the partition mounted at `/var/lib/mithril` (size determined by `100 - LVM_PCT`) to hold the downloaded `.tar.zst` file (e.g., ~95GiB+). You *also* need enough space in the thin pool (size determined by `LVM_PCT`) to hold the *uncompressed* accounts database built on `/mnt/accounts_master` (which can easily exceed 700GiB). If your disk isn't large enough for both, consider setting `LVM_PCT` high (e.g., 95) and modifying Step 3/4 to download/read the `.tar.zst` from a *different disk* entirely. `LVM_PCT=80` (giving ~179G for downloads and ~713G for the DB pool) is a starting point only if your disk is sufficiently large (e.g., 1TB+).
    * `SNAPSHOTS_DIR_OWNER`: User:group for the `/var/lib/mithril/snapshots` directory in `setup_mithril_lvm.sh` (e.g., `ubuntu:ubuntu`).
    * `SNAPSHOT_MOUNT_BASE`: Base path for snapshot mounts in `manage_mithril_snapshot.sh` (e.g., `/mnt`).

---

## Workflow Steps

1.  **Wipe Disk (Optional - Use with extreme caution!)** ⚠️

    * This step completely erases the target NVMe drive specified in the script. Only run this if you are starting fresh or need to reset everything on that disk.
    ```bash
    # Make sure DEVICE in setup_mithril_lvm.sh points to the correct NVMe drive!
    sudo ./setup_mithril_lvm.sh wipe
    # Follow prompts - requires typing 'yes'
    ```

2.  **Setup LVM Infrastructure**

    * This partitions the disk, creates the LVM Volume Group, Thin Pool, the master Logical Volume (`accounts_master`), formats filesystems, mounts `/mnt/accounts_master` & `/var/lib/mithril`, creates `/var/lib/mithril/snapshots` subdir, and sets up `/etc/fstab` for auto-mounting.
    * > **ℹ️ Note:** Ensure `LVM_PCT` in the script provides enough space for `/var/lib/mithril` (for Step 3) *and* for `/mnt/accounts_master` (for Step 4), considering the **Disk Space Warning** above.
    ```bash
    sudo ./setup_mithril_lvm.sh setup
    # Verify mounts after completion, e.g., with 'df -h /mnt/accounts_master /var/lib/mithril'
    ```

3.  **Download Solana Snapshot (`.tar.zst`)**

    * Use your python script (or another method) to find and download the desired Solana network snapshot file into the designated directory created by the setup script.
    * > **ℹ️ Note:** This requires `/var/lib/mithril` (Partition 2) to have sufficient space (e.g., ~95GiB+). Verify the `LVM_PCT` setting provides this.
    ```bash
    # Example using your python script
    python3 snapshot-finder.py --snapshot_path /var/lib/mithril/snapshots --version 2. --max_latency 150
    # Make note of the exact filename downloaded (e.g., snapshot-XYZ.tar.zst) for the next step
    ```

4.  **Build Initial AccountsDB on Master LV** ✅

    * Run the Mithril verifier in snapshot mode to unpack the downloaded `.tar.zst` file and build the accounts database onto the persistent `accounts_master` LVM volume.
    * > **⚠️ Warning:** This process writes the *uncompressed* database. Ensure the thin pool (Partition 1, size set by `LVM_PCT`) is large enough, otherwise you will get "no space left on device" errors.
    * > **ℹ️ Note:** This command likely requires `sudo` to write to `/mnt/accounts_master`.
    ```bash
    # Replace <downloaded_snapshot_filename.tar.zst> with the actual filename from Step 3
    # Replace <YOUR_API_KEY> with your actual Helius API key
    sudo ~/mithril/mithril verifier \
      --snapshot \
      --path /var/lib/mithril/snapshots/<downloaded_snapshot_filename.tar.zst> \
      --out /mnt/accounts_master \
      --rpc [https://mainnet.helius-rpc.com/?api-key=](https://mainnet.helius-rpc.com/?api-key=)<YOUR_API_KEY>
    ```

5.  **Create Temporary LVM Snapshot**

    * Use the snapshot management script to create a point-in-time LVM snapshot of the `accounts_master` volume (which now contains the built database). This snapshot will be mounted read-write for the next step.
    * `<name>` (e.g., `run1`) determines the snapshot LV name and default mount point (`/mnt/<name>`).
    ```bash
    # Creates snapshot 'run1', activates it, checks filesystem, mounts it at /mnt/run1
    sudo ./manage_mithril_snapshot.sh snapshot run1
    ```

6.  **Run Mithril Process on LVM Snapshot**

    * Run your Mithril process (e.g., verifying block ranges) against the *LVM snapshot's mount point* (`/mnt/run1` in this example). Any changes made by Mithril will be written only to the snapshot, leaving `/mnt/accounts_master` untouched.
    * > **ℹ️ Note:** This command might require `sudo` depending on permissions and Mithril's requirements.
    ```bash
    # Replace <YOUR_API_KEY> and adjust slots as needed
    sudo ~/mithril/mithril verifier \
      --accountsdb \
      --path /mnt/run1 \
      --startslot 337648943 \
      --endslot   337648953 \
      --rpc [https://mainnet.helius-rpc.com/?api-key=](https://mainnet.helius-rpc.com/?api-key=)<YOUR_API_KEY>
    ```

7.  **Clean Up LVM Snapshot** ✅

    * Once your Mithril run is complete, remove the temporary LVM snapshot using the management script. This frees up space within the thin pool that was consumed by the changes written during Step 6.
    ```bash
    sudo ./manage_mithril_snapshot.sh remove_snapshot run1
    ```

---

**Repeating Runs:** For subsequent runs against the same base `accounts_master` database, repeat **Steps 5, 6, and 7**. You only need to perform Steps 1-4 again if you need to completely reset the disk or build the accounts database from a newer Solana network snapshot.
