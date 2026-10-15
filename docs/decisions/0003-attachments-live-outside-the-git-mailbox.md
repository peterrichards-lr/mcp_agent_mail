STATUS: Accepted 2026-10-15

# 0003. File attachments live in a blob store, outside the Git mailbox

## Decision

Agents on different machines exchange files by uploading them to the server and attaching them to messages
by id. The bytes go into a content-addressed blob store next to the storage root, not into the per-project Git
mailbox, and they never pass through a model.

Owner, 2026-10-15, on the compatibility-matrix use case (a coordinator collects reports from agents on other
machines):

> "Sure in this case it is fine but what if we need attachments. How can that be supported?"

> "The thing is the master agent needs to review the reports and run a sync report before they are committed
> to the repo"

The owner set the requirement: reports have to reach the coordinator intact, and it reviews them before
anything is committed. **The storage mechanism was chosen in session, not by the owner.** The session put
files in a blob store rather than in Git for three reasons:
- reports can be large;
- the coordinator, not the mailbox, decides what gets committed, and to which repo;
- downloads are verified against SHA-256 and arrive byte for byte.

## Cost

- Attachments aren't in the Git audit trail that messages are in. Backups have to carry them separately: the `archive` preset includes `attachment_blobs/`.
- Uploads are capped (`ATTACHMENT_MAX_BYTES`).
- Moving a file takes a client script (`scripts/hooks/agent_mail_files.py`), not just MCP tools.

## Enforced by

- `src/mcp_agent_mail/app.py`: `_store_file_attachment`, `_blob_root` and `_blob_path`, plus the `attachment_ids` parameter on `send_message` and `reply_message`.
- `tests/test_file_attachments.py`.

## Revisit when

Attachments need the same history and audit as messages, for example a legal hold on reports. Or when a
real object store (S3 or similar) replaces the local disk.
