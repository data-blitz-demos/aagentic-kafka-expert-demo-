# Prompts

This directory stores every LLM-facing prompt used by the application.

## Files

- `kafka_only_message.txt`: refusal text used when a user asks a non-Kafka question.
- `system_prompt.txt`: Agent 1 policy for read-only Kafka monitoring and labeled findings.
- `remediation_system_prompt.txt`: Agent 2 policy for authorized, minimal, verified repairs.
- `cluster_state_prompt.txt`: prompt for full Kafka cluster state runs.
- `auto_fix_prompt_template.txt`: template for user-clicked `Fix:` remediations.
- `graphrag_extract_prompt.txt`: prompt for extracting graph edges from PDF chunks.
- `graphrag_query_prompt.txt`: prompt for answering from extracted graph context.
