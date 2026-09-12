# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==5.5.0",
#     "ipython==9.3.0",
#     "marimo",
# ]
# ///

import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import json

    latency_records = []

    # Path to replay_timings.jsonl - update to point to your run's log directory
    # Example: /mnt/mithril-logs/20260115-120000Z_abc123_def456/replay_timings.jsonl
    import sys
    if len(sys.argv) > 1:
        timings_path = sys.argv[1]
    else:
        timings_path = "/mnt/mithril-logs/latest/replay_timings.jsonl"

    with open(timings_path) as f:
        for l in f:
            record = json.loads(l)
            record["SlotCount"] = len(latency_records)
            # Flatten the structure.
            flat_record = {}
            for k, v in record.items():
                if k == "VoteRewardDetails" and type(v) == dict:
                    for detail_key, detail_value in v.items():
                        flat_key = f"VoteReward{detail_key}"
                        if type(detail_value) == dict and "SumNanoseconds" in detail_value:
                            flat_record[flat_key] = detail_value["SumNanoseconds"] / 1_000_000
                        else:
                            flat_record[flat_key] = detail_value
                elif type(v) == dict and "SumNanoseconds" in v:
                    flat_record[k] = v["SumNanoseconds"] / 1_000_000
                else:
                    flat_record[k] = v
            latency_records.append(flat_record)

    len(latency_records)
    latency_records[0]
    return (latency_records,)


@app.cell
def _():
    import marimo as mo
    import altair as alt
    return alt, mo


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    # These three bars are exact, disjoint wall-clock intervals. SlotReplay is
    # drawn as a line because it is their inclusive total and must not be
    # stacked with them.
    wall_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "PreprocessBlock",
                "ProcessBlock",
                "PostProcessBlock",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    slot_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("SlotReplay:Q", title="Latency (ms)"),
            tooltip=["Slot:Q", "SlotReplay:Q"],
        )
    )
    wall_chart = alt.layer(wall_components, slot_total).properties(
        title="Exact slot wall time (SlotReplay line equals the stacked phases)"
    )
    mo.ui.altair_chart(wall_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    # Only disjoint top-level ProcessBlock phases are stacked. Prepared planner
    # build may overlap account loading; wait and dispatch are nested within
    # TxLoop, so all three are overlaid as diagnostic lines.
    # SignatureVerificationJoin is only the final blocking wait; the existing
    # Sigverify metric is summed worker time that overlaps these wall phases.
    process_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "TransactionExecutionPlan",
                "TransactionStatusValidation",
                "DependencyPlannerPreparation",
                "LoadBlockAccounts",
                "SlotCtxSetup",
                "TxLoop",
                "Reward",
                "Rent",
                "RunIncinerator",
                "AlpenglowFooterClock",
                "AlpenglowVoteRewards",
                "CompileWritableAndModifiedAccts",
                "EnsureParentAccountsForModified",
                "BankHash",
                "AlpenglowFooterVerification",
                "BlockUpdateAccounts",
                "TransactionStatusCommit",
                "SignatureVerificationJoin",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    process_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("ProcessBlock:Q", title="Latency (ms)"),
            tooltip=["Slot:Q", "ProcessBlock:Q"],
        )
    )
    planner_detail = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "DependencyPlannerBuild",
                "DependencyPlannerWait",
                "DependencyPlannerDispatch",
            ],
            as_=["Nested timer", "Latency"],
        )
        .mark_line(point=True, strokeDash=[5, 3])
        .encode(
            x="SlotCount:O",
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Nested timer:N",
            tooltip=["Slot:Q", "Nested timer:N", "Latency:Q"],
        )
    )
    process_chart = (
        alt.layer(process_components, process_total, planner_detail)
        .resolve_scale(color="independent")
        .properties(
            title="ProcessBlock detail (black total; prepared build may overlap load, wait/dispatch are nested in TxLoop)"
        )
    )
    mo.ui.altair_chart(process_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    # These sub-phases are disjoint, but do not cover every bookkeeping action.
    # Keep PostProcessBlock as an overlaid total so the residual remains visible.
    post_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            ["TransactionStatusView", "ChainTipUpdate", "ResumeContext"],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    post_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("PostProcessBlock:Q", title="Latency (ms)"),
            tooltip=["Slot:Q", "PostProcessBlock:Q"],
        )
    )
    post_chart = alt.layer(post_components, post_total).properties(
        title="PostProcessBlock detail (inclusive total shown as black line)"
    )
    mo.ui.altair_chart(post_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    # AccountsDeltaHash and the LtHash phases are feature-dependent alternatives;
    # BankHashFinalize follows either path. BankHash is their inclusive total.
    bankhash_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "AccountsDeltaHash",
                "LtHashDedupe",
                "LtHashWorkerCompute",
                "LtHashPartialReduce",
                "BankHashFinalize",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    bankhash_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("BankHash:Q", title="Latency (ms)"),
            tooltip=["Slot:Q", "BankHash:Q"],
        )
    )
    bankhash_chart = alt.layer(bankhash_components, bankhash_total).properties(
        title="BankHash/LtHash detail (BankHash inclusive total shown as black line)"
    )
    mo.ui.altair_chart(bankhash_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    reward_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "VoteRewardValidatorPreparation",
                "VoteRewardSkipCertificateValidation",
                "VoteRewardNotarCertificateValidation",
                "VoteRewardFinalCertificateDecode",
                "VoteRewardFinalCertificateValidation",
                "VoteRewardStatePreparation",
                "VoteRewardAccountMutation",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    reward_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("AlpenglowVoteRewards:Q", title="Latency (ms)"),
            tooltip=["Slot:Q", "AlpenglowVoteRewards:Q"],
        )
    )
    reward_chart = alt.layer(reward_components, reward_total).properties(
        title="AlpenglowVoteRewards detail (inclusive total shown as black line)"
    )
    mo.ui.altair_chart(reward_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    tx_chart = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "InstructionsAndAccountMetasFromTx",
                "ComputeBudgetExecutionInstructions",
                "AccountsFromTx",
                "PreBalanceDivergenceCheck",
                "CalcAndDeductFees",
                "ReadRentSysvar",
                "PreTxRentStates",
                "IxLoop",
                "PostTxRentStates",
                "PostBalanceDivergenceCheck",
                "TxUpdateAccounts",
                "TxFailedUpdateAccounts",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(x="SlotCount:O", y="Latency:Q", color="Phase:N")
    )
    mo.ui.altair_chart(tx_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    publication_source = alt.Chart(
        alt.InlineData(values=latency_records)
    ).transform_calculate(
        TouchedMiB="datum.TxPublicationTouchedAccountBytes / 1048576",
        TouchedMicrosPerAccount="datum.TxPublicationTouchedAccounts > 0 ? datum.TxPublishTouchedAccountState * 1000 / datum.TxPublicationTouchedAccounts : 0",
    )
    publication_components = (
        publication_source
        .transform_fold(
            [
                "TxPublishRecordWritableAcct",
                "TxPublishTouchedAccountState",
                "TxPublishStakeVoteBookkeeping",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Summed transaction latency (ms)"),
            color="Phase:N",
            tooltip=[
                "Slot:Q",
                "Phase:N",
                "Latency:Q",
                "TxPublicationTouchedAccounts:Q",
                alt.Tooltip("TouchedMiB:Q", format=".2f"),
                alt.Tooltip("TouchedMicrosPerAccount:Q", format=".3f"),
            ],
        )
    )
    publication_total = (
        publication_source
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("TxUpdateAccounts:Q", title="Summed transaction latency (ms)"),
            tooltip=[
                "Slot:Q",
                "TxUpdateAccounts:Q",
                "TxPublicationTouchedAccounts:Q",
                alt.Tooltip("TouchedMiB:Q", format=".2f"),
                alt.Tooltip("TouchedMicrosPerAccount:Q", format=".3f"),
            ],
        )
    )
    publication_chart = alt.layer(publication_components, publication_total).properties(
        title="Successful transaction publication (inclusive total shown as black line)"
    )
    mo.ui.altair_chart(publication_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    failed_components = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "TxFailedPublicationPreparation",
                "TxFailedPayerPublication",
                "TxFailedNoncePublication",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(
            x=alt.X("SlotCount:O", title="Replayed slot index"),
            y=alt.Y("Latency:Q", title="Summed transaction latency (ms)"),
            color="Phase:N",
            tooltip=["Slot:Q", "Phase:N", "Latency:Q"],
        )
    )
    failed_total = (
        alt.Chart(alt.InlineData(values=latency_records))
        .mark_line(point=True, color="black")
        .encode(
            x="SlotCount:O",
            y=alt.Y("TxFailedUpdateAccounts:Q", title="Summed transaction latency (ms)"),
            tooltip=["Slot:Q", "TxFailedUpdateAccounts:Q"],
        )
    )
    failed_chart = alt.layer(failed_components, failed_total).properties(
        title="Failed transaction publication (inclusive total shown as black line)"
    )
    mo.ui.altair_chart(failed_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    ix_chart = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "GetNextIxCtx",
                "NextIxCtxConfigure",
                "IxPush",
                "IxPop",
                "ExecIxResolveNativeProgram",
                "ExecIxNativeProgramSystem",
                "ExecIxNativeProgramStake",
                "ExecIxNativeProgramVote",
                "ExecIxNativeProgramComputeBudget",
                "ExecIxNativeProgramBpfLoader2",
                "ExecIxNativeProgramBpfLoaderDeprecated",
                "ExecIxNativeProgramBpfLoaderUpgradeable",
                "ExecIxNativeProgramZkElgamalProof",
                "ExecIxNativeProgramEd25519Precompile",
                "ExecIxNativeProgramSecp256kPrecompile",
                "FixupInstructionsSysvarAccount",
                "InstructionAccountsFromAccountMetas",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(x="SlotCount:O", y="Latency:Q", color="Phase:N")
    )
    mo.ui.altair_chart(ix_chart)
    return


@app.cell(hide_code=True)
def _(alt, latency_records, mo):
    sbpf_chart = (
        alt.Chart(alt.InlineData(values=latency_records))
        .transform_fold(
            [
                "SbpfInterpreterNew",
                "SbpfInterpreterRun",
                "AddProgramToCache",
                "GetProgramAccount",
                "GetProgramDataCached",
                "GetProgramDataUncachedAccountsDb",
                "GetProgramDataUncachedAccounts",
                "GetProgramDataUncachedMarshal",
            ],
            as_=["Phase", "Latency"],
        )
        .mark_bar()
        .encode(x="SlotCount:O", y="Latency:Q", color="Phase:N")
    )
    mo.ui.altair_chart(sbpf_chart)
    return


if __name__ == "__main__":
    app.run()
