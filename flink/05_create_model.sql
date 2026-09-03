-- 05: LLM model for the Flink Streaming Agent.
--
-- The dispatcher agent (job 06) calls this model through AI_RUN_AGENT. The model
-- is a first-class Flink object created from a connection to a hosted LLM. Run
-- this once, before job 06.
--
-- In the F1 demo the connection + model are provisioned by Terraform
-- (terraform/modules/llm). Here we show the SQL directly so it runs in the Flink
-- workspace. Pick ONE provider block below, fill in your endpoint/credentials,
-- then create the model.
--
-- Replace `<catalog>` with your environment name and `<database>` with your Kafka
-- cluster name (the same values shown at the top of the Flink workspace), or set
-- them once with:  SET 'sql.current-catalog' = '<env>';  SET 'sql.current-database' = '<cluster>';

-- ---- Option A: AWS Bedrock (Anthropic Claude) — matches the F1 demo ----
CREATE CONNECTION `llm-dispatcher-connection`
WITH (
  'type'     = 'BEDROCK',
  'endpoint' = 'https://bedrock-runtime.us-east-1.amazonaws.com/model/us.anthropic.claude-sonnet-4-5-20250929-v1:0/invoke',
  'aws-access-key' = '<AWS_ACCESS_KEY>',
  'aws-secret-key' = '<AWS_SECRET_KEY>'
);

-- ---- Option B: OpenAI (uncomment to use instead of Bedrock) ----
-- CREATE CONNECTION `llm-dispatcher-connection`
-- WITH (
--   'type'     = 'OPENAI',
--   'endpoint' = 'https://api.openai.com/v1/chat/completions',
--   'api-key'  = '<OPENAI_API_KEY>'
-- );

-- ---- Option C: Azure OpenAI (uncomment to use instead) ----
-- CREATE CONNECTION `llm-dispatcher-connection`
-- WITH (
--   'type'     = 'AZUREOPENAI',
--   'endpoint' = 'https://<resource>.openai.azure.com/openai/deployments/<deployment>/chat/completions?api-version=2024-10-21',
--   'api-key'  = '<AZURE_OPENAI_API_KEY>'
-- );

-- ---- Option D: Google AI / Gemini (uncomment to use instead) ----
-- Get an API key at https://aistudio.google.com/apikey (works with Gemini Pro).
-- Swap the model in the path (gemini-2.0-flash, gemini-1.5-pro, ...).
-- CREATE CONNECTION `llm-dispatcher-connection`
-- WITH (
--   'type'     = 'GOOGLEAI',
--   'endpoint' = 'https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:generateContent',
--   'api-key'  = '<GOOGLEAI_API_KEY>'
-- );

-- CREATE MODEL — set 'provider' to match the connection you created above.
-- Bedrock (Option A, default):
CREATE MODEL `llm_dispatcher_model`
INPUT  (`prompt` STRING)
OUTPUT (`response` STRING)
WITH (
  'provider'                 = 'bedrock',
  'task'                     = 'text_generation',
  'bedrock.connection'       = 'llm-dispatcher-connection',
  'bedrock.params.max_tokens' = '1024'
);

-- Google AI / Gemini (Option D) — use this WITH clause instead of the one above:
-- CREATE MODEL `llm_dispatcher_model`
-- INPUT  (`prompt` STRING)
-- OUTPUT (`response` STRING)
-- WITH (
--   'provider'           = 'googleai',
--   'task'               = 'text_generation',
--   'googleai.connection' = 'llm-dispatcher-connection'
-- );
