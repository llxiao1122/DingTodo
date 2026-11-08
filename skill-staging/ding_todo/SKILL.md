---
name: ding_todo
description: "Use for DingTalk todos and Cifra REVIEW handoffs."
version: 0.1.0
author: "李林骁, Hermes Agent"
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [dingtalk, todo, task-creation, verification]
    related_skills: []
---

# ding_todo Skill

Cifra 机器人承载 DingTodo 的询问、确认和处理状态反馈；主人也希望在该钉钉会话中直接与 Hermes 上的 Cifra 交流。按当前授权分别核验日常对话和 REVIEW 处理链路，不因机器人名称或窗口一致就认定由同一后端处理。保持任务与回复的精确关联；自动回复、回调切换及生产启用分别核验并遵守授权边界。

Create exactly the number of DingTalk personal todos the user requests from a confirmed task source, and verify each remote record by reading it back. Treat every task in a batch as an independent write with its own taskId and verification. Do not send a group message or add comments, attachments, reminders, tags, or follow-up mutations unless the user explicitly requests them.

## When to Use

- The user explicitly asks to create a DingTalk todo.
- A task source has been read from DingTalk group messages or another verified source.
- The owner replies to a configured Cifra private-chat REVIEW inquiry and asks Cifra to process it; load [Cifra REVIEW human inquiry](references/review-inquiry.md).
- The user has identified the executor, or the executor is the authenticated current user.
- Do not create a task when the title or executor is still ambiguous.
- Do not send or reply to a group message as part of this workflow.

## Prerequisites

- Run commands through the Hermes `terminal` tool.
- Confirm the active DingTalk identity without changing it:

```bash
dws auth status --format json
dws contact user get-self --format json
```

- Load the current official `dingtalk-todo` Skill when selecting a DWS entry point, especially after an upgrade. Use a single-purpose Shortcut when it fully covers the authorized operation; retain atomic creation when comments, attachments, member changes, sub-tasks, or a controlled ledger handoff form a composite lifecycle. Do not assume every Shortcut supports deadline and priority fields.
- Inspect only the exact leaf contract when its parameters or safety semantics are uncertain:

```bash
dws schema --cli-path "todo task create" --compact --format json
```

Use that known leaf's Help only if Schema is unavailable; do not guess aliases or enumerate parent catalogs to repair a known command.

- Use a live `date` command and ISO-8601 timestamps with the intended timezone. For the user's China work context, use `+08:00`.

## Quick Reference

The direct create command accepts the applicable creation fields:

```bash
dws todo task create \
  --title "<precise title>" \
  --executors "<userId>[,<userId>...]" \
  --due "<YYYY-MM-DD>T<HH:MM:SS>+08:00" \
  --priority 10|20|30|40 \
  --format json
```

Priority values are `10` low, `20` normal, `30` higher, and `40` urgent. `--recurrence` is also available, but it must not be set for a one-off test or operational task unless recurring execution was explicitly requested.

## Procedure

1. Read and preserve the source task. Capture its original wording, source time, group, stated assignee, and stated deadline. Use `ding_qun` for a DingTalk group source rather than guessing from memory.
2. Resolve every executor immediately before writing. For the current user, use `userId` from `contact user get-self`; for another person, use a unique live contact search and verify the exact name. Never reuse a staff ID from a local work database without checking it against DWS, because stored mappings can be stale. Search relevant open tasks before creating; read back plausible exact/near matches and distinguish same-source idempotency from distinct-message same-work duplicates. State whether the search covers the current user's visible tasks or a wider organization scope—a zero in a personal list does not prove nobody else has the work. Stop or clarify if a plausible duplicate cannot be resolved.
3. Resolve deadline semantics and build a faithful title. Preserve an explicit date and time exactly. If only a date is given but DWS requires a timestamp, use a documented/approved all-day convention and disclose the conversion; otherwise leave the structured due field unset while retaining the date in the title, or ask when a structured deadline is required. Do not silently invent a workday cutoff or treat 17:00/23:59 as user-specified. For past deadlines, ask rather than silently creating an overdue task. If a todo has multiple executors or sub-work assignments, retain each person's allocation and the acceptance criterion in the title; a shared executor list alone does not specify who owns which part. The create contract has no free-form description, so ask before moving essential detail into a separate comment or another write. Mark exploration tasks as tests in the title.
4. Fill applicable create fields: title, executor list, due time when supported by an established deadline convention, and priority. Leave recurrence unset for a one-off task. The create contract does not include a free-form description, attachment, comment, reminder, or tag field; do not perform those separate writes unless requested.
5. Preview the exact payload before the external write:

```bash
dws todo task create \
  --title "<title>" --executors "<userId>" \
  --due "<ISO-8601 due time>" --priority <10|20|30|40> \
  --dry-run --format json
```

Inspect the current CLI help/schema and validate the returned preview shape rather than assuming fields are top-level; some versions nest values under `arguments.PersonalTodoCreateVO`. Confirm the executor IDs, `dueTime`, `priority`, `subject`, and `executed:false` against the intended request.
6. For a manual backfill of a message managed by the resident pipeline, first read its exact source action and any external linkage. Use the controlled writer to claim `conversation_id + message_id + action_key=todo` before remote creation, require its durable `action_id`, and reconcile an existing or uncertain claim instead of creating again. This keeps manual backfill and the resident on the same idempotency boundary. Execute each approved command once, capturing its stable `taskId` before continuing; immediately link that ID to the claimed action before optional follow-up writes, then read back the exact action as well as the remote task. For a batch, track success or uncertainty per item; never replay the whole batch after a partial result. If a create times out or its response is uncertain, query for the exact task before retrying because create idempotency is unknown. Do not call any `dws chat ... send`, reply, or event-subscription command. For a numbered human-confirmed batch, bind each create/ignore decision and executor confirmation to that item's exact ordinal; never let a message-wide keyword check authorize another item. If the selected item's decision or executor is missing, ambiguous, or contradictory, do not write.

When handling an existing resident-pipeline REVIEW through Cifra, follow [Cifra REVIEW human inquiry](references/review-inquiry.md); keep that controlled private-chat handoff separate from this direct todo-creation route. Trace the live inbox worker and resolver before describing automation: a timer that records a reply does not itself prove automatic acknowledgement, resolution, or todo creation.

### Task-feedback closure

1. Before asking, verify the bot, owner, private conversation, source organization and exact task, then check for an existing answer and competing REVIEW requests. Reconcile an uncertain send against complete bounded history; never resend merely because a client timed out.
2. Preserve each reply verbatim with its message identity, sender, conversation and source time. Associate it with the exact inquiry and task only when the complete history and identity support the link; otherwise retain human review. Distinguish manual association from a verified automatic receiver. Verify the responding backend and its access to the original inquiry independently: a bot's visible name or successful unrelated todo operations do not establish that its current responder received the inquiry or can associate its replies.
3. Obtain the owner's separate authorization for a work-ledger write when required by the task. Use a bounded approved writer and stable source-derived event identity; read back every field and reconcile unknown outcomes before any retry. Label `owner_reported` as the owner's statement, not independent proof. Do not alter the DingTalk task status without separate authority.
4. Validate delivery, reply association, committed write with exact read-back, and a genuinely new Hermes conversation separately. Query by source organization and task ID, verify the original words and evidence status, and check whether the conversation avoids repeating an answered question. A new MCP process, synthetic test, or session with a custom scoped tool proves only that narrower route, not the ordinary production session. Do not mark the closure complete while an expressly required route is unverified. If all specified criteria for a one-off closure pass through a permitted manual handoff, report that one-off completion and explicitly distinguish it from unverified automatic REVIEW handling or production enablement; do not silently add automation as a new acceptance condition.

7. Require a stable `taskId` from the create response. Verify the exact remote task:

```bash
dws todo task get --task-id "<taskId>" --format json
```

8. Compare the read-back `subject`, `taskId`, `executorInfos`, `dueTime`, `priority`, and `isDone` with the request. Convert timestamps with a tool when reporting local time; do not rely on mental epoch conversion.
9. Report each task ID, exact title, executor names, due time, priority, unfinished status, any date-only conversion, the scope of the duplicate search, and whether a group message was sent. Keep tasks unfinished unless separately asked to complete them. For the Cifra REVIEW private-chat workflow, follow the linked response contract instead: do not list task details in the bot's completion message.

## Workgroup routing and test gate

For DingTodo workgroup tasks, default the executor to the authenticated current user. Do not use the historical area-duty or responsibility matrix to choose executors or block a task. If the user explicitly requests another executor for a particular task, follow that instruction only after verifying the identity and the requested assignment. Preserve the source wording so that any workgroup or named-person responsibilities remain visible as context.

- Resolve the current user through DWS immediately before creation; do not rely on a stored ID. Mark an explicitly requested test in its title.
- Preserve the original notice, source group, sender, time, and messageId in the source ledger. Add a todo source comment only when the user or explicitly approved production workflow authorizes that separate write; inspect the live CLI help for its actual command and read the comment back. A request to add one todo does not implicitly authorize comments or attachments.
- When the user explicitly requests source images/files, resolve every message resource to a local file before the attachment write. For a message image, run `dws chat +messages-resource-download --resource-id <mediaId> --type mediaId --message-id <messageId> --open-conversation-id <conversationId> --output . --format json` from a Hermes scratch working directory; then run `dws todo task add-attachment --task-id <taskId> --file <localPath> --format json` and verify with `dws todo task list-attachment --task-id <taskId> --format json`. Attachments are separate non-idempotent writes: do not probe an unknown task, do not silently retry, preserve each returned attachmentId, and keep the action in `REVIEW` if download, upload, or attachment read-back fails. Keep the original notice and mediaId in the todo comment as audit evidence.
- Perform scope checks, date parsing, deduplication, and validation first. Send only unresolved title/context cases to an LLM, and keep them in `REVIEW` until the required fields are returned and checked.

## Pitfalls

- A successful create response without a stable `taskId` is not verified completion.
- The service may represent a string DingTalk user ID as an internal numeric ID on read-back. Verify `executorInfos.name` and the resolved identity, not only the raw ID representation.
- `dws todo task get` does not return reminder rules. Never claim a reminder was verified through task detail.
- Adding participants, attachments, comments, tags, or reminders is a separate external write. Do not add them merely because they exist in the broader Todo catalog.
- A recurring rule creates repeated behavior and is not a harmless way to fill an optional field.
- A task title based on a group message is not proof that the group was notified; this workflow intentionally does not send a group message.

## Verification

Normalize the actual selected command's response contract before evaluating success: atomic commands can use `success/result`, while Shortcuts can use `ok/outcome/data`. Require a stable `taskId` and a successful exact-task read-back under that contract; do not reject a valid Shortcut solely because it lacks an atomic `success` field. Confirm the task is still incomplete and that every requested applicable field matches the read-back record.
