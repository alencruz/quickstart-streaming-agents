# Lab4: Public Sector Insurance Claims Fraud Detection Using Confluent Intelligence

![FEMA Fraud Detection](./assets/lab4/lab4-architecture.png)

This demo showcases an intelligent, real-time fraud detection system that autonomously identifies suspicious claim patterns in FEMA disaster assistance applications. Built on [Confluent Intelligence](https://www.confluent.io/product/confluent-intelligence/), the system combines stream processing, anomaly detection, and AI-powered analysis to detect organized fraud rings and policy violations in real-time.

## Running this lab in GitHub Codespaces (optional but recommended for users with restricted access to their device)

You can run this lab in two ways:

- **Locally** on your own machine (original path), or
- **In GitHub Codespaces**, using only a web browser and a GitHub account.

When using GitHub Codespaces:

1. Login to your GitHub account (must **not** have Confluent organizational access).
2. Open this repository on GitHub.
3. Click **Code → Codespaces → Create codespace** on the branch you want to use (master).
4. Wait for the dev container to build and the Codespace to open in your browser.
5. Run all shell commands in this lab (for example `uv run deploy`) from the **Integrated Terminal** inside the Codespace.
6. Use your **local browser** (outside Codespaces) to access:
   - The [Flink UI](https://confluent.cloud/go/flink)
   - Other Confluent Cloud UI pages
   - AWS / Azure portals as needed for credentials

## Prerequisites

You can either:

- **Run locally** and install the tools on your machine, or
- **Run in GitHub Codespaces**, where these tools are preinstalled in a dev container

**Local installation on macOS**

```bash
brew install uv git python && brew tap hashicorp/tap && brew install hashicorp/tap/terraform && brew install --cask confluent-cli
```

**Local installation on Windows**
```powershell
winget install astral-sh.uv Git.Git Hashicorp.Terraform ConfluentInc.Confluent-CLI Python.Python
```

Once software is installed (or your Codespace is ready), you'll need:
- **Confluent Cloud credentials** 
  - [![Sign up for Confluent Cloud](https://img.shields.io/badge/Sign%20up%20for%20Confluent%20Cloud-007BFF?style=for-the-badge&logo=apachekafka&logoColor=white)](https://www.confluent.io/get-started/?utm_campaign=tm.pmm_cd.q4fy25-quickstart-streaming-agents&utm_source=github&utm_medium=demo)
- **LLM API keys:** AWS Bedrock API keys **OR** Azure OpenAI endpoint + API key
  - **Workshop participants:** The LLM keys will be provided for you
  - **Easy key creation:** Run `uv run api-keys create` to quickly auto-generate LLM credentials using your cloud account

---

## Deploy the Demo

First, if running locally, clone the repo (if using Codespace skip  this step - the repo is cloned automatically and your terminal starts inside the project):

```bash
git clone https://github.com/confluentinc/quickstart-streaming-agents.git
cd quickstart-streaming-agents
```
Log in to Confluent Cloud
```bash
confluent login
```
Once you have your LLM API credentials ready, run the deployment script and choose **Lab4** when prompted:

```bash
uv run deploy
```
Run this from your **development environment** (local terminal or Codespace terminal).

The deployment script will prompt you for your:
- Cloud provider (AWS/Azure)
- LLM API keys (Bedrock keys or Azure OpenAI endpoint/key)

Select **"Lab 4: FEMA Fraud Detection"** from the menu.

---

## Use Case Walkthrough

### Data Generation

The Lab4 Terraform automatically publishes ~36,000 synthetic claims across 8 Florida cities. The hurricane claims begin 14 days before the current date (on the day of the hurricane), and continue through today.

The data includes:
- **`claims`** table – synthetic disaster assistance claims with applicant info, damage assessments, claim amounts, and detailed narratives

**Data Pattern:**
- **7 cities** show normal exponential decay (claims decrease over time)
- **1 city (Naples)** shows an anomalous spike in the final 2 days (Days 13-14), containing a mix of claims: some with fraud indicators, others with policy violations, and others still with fully legitimate claims.

---

### 0. Visualize the Anomaly

Before running anomaly detection, you can view the raw claim patterns for yourself by running these two queries in the [Flink UI](https://confluent.cloud/go/flink):

**All other regions (normal decay):**

```sql
SELECT
    window_start,
    window_end,
    city,
    SUM(CAST(claim_amount AS DOUBLE)) AS total_claims_amount,
    COUNT(*) AS claim_count
FROM TABLE(
    TUMBLE(TABLE claims, DESCRIPTOR(claim_timestamp), INTERVAL '1' HOUR))
WHERE city <> 'Naples'
GROUP BY window_start, window_end, city;
```

**Anomaly region — Naples only (claims actually *increasing* on days 8-9):**

```sql
SELECT
    window_start,
    window_end,
    SUM(CAST(claim_amount AS DOUBLE)) AS total_claims_amount,
    COUNT(*) AS claim_count
FROM TABLE(
    TUMBLE(TABLE claims, DESCRIPTOR(claim_timestamp), INTERVAL '1' HOUR))
WHERE city = 'Naples'
GROUP BY window_start, window_end;
```

After running each query, click the **Switch to Time Series** chart in the UI to visualize the results:

<img src="./assets/lab4/switch_to_time_series.png" width="40%" alt="Switch to Time Series view" />

All other regions show a steady downward slope as claims taper off post-disaster. Naples follows the same pattern initially, then spikes sharply upward — the anomaly:

![All regions vs anomaly region](./assets/lab4/all_regions_vs_anomaly_region.png)

---

### 1. Detect Fraud Spikes Using `ML_DETECT_ANOMALIES`

This step identifies unexpected surges in claim amounts for each city in real time using Flink's built-in anomaly detection function. We analyze claim amounts over 3-hour windows and compare them against expected baselines derived from historical trends.

Read the [blog post](https://docs.confluent.io/cloud/current/ai/builtin-functions/detect-anomalies.html) and view the [documentation](https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html#flink-sql-ml-anomaly-detect-function) on Flink anomaly detection for more details.

**Run this query in the [Flink UI](https://confluent.cloud/go/flink) to create the anomaly detection table:**

```sql
SET 'sql.state-ttl' = '14 d';

CREATE TABLE claims_anomalies_by_city AS
WITH windowed_claims AS (
    SELECT
        window_start,
        window_end,
        window_time,
        city,
        COUNT(*) AS claim_count,
        SUM(CAST(claim_amount AS DOUBLE)) AS total_claim_amount,
        CAST(ROUND(AVG(CAST(claim_amount AS DOUBLE))) AS BIGINT) AS avg_claim_amount,
        SUM(CAST(damage_assessed AS DOUBLE)) AS total_damage_assessed
    FROM TABLE(
        TUMBLE(TABLE claims, DESCRIPTOR(claim_timestamp), INTERVAL '6' HOUR)
    )
    GROUP BY window_start, window_end, window_time, city
),
anomaly_detection AS (
    SELECT
        city,
        window_time,
        claim_count,
        total_claim_amount,
        avg_claim_amount,
        total_damage_assessed,
        ML_DETECT_ANOMALIES(
            CAST(total_claim_amount AS DOUBLE),
            window_time,
            JSON_OBJECT(
                'minTrainingSize' VALUE 8,
                'maxTrainingSize' VALUE 50,
                'confidencePercentage' VALUE 95.0,
                'enableStl' VALUE FALSE
            )
        ) OVER (
            PARTITION BY city
            ORDER BY window_time
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly_result
    FROM windowed_claims
)
SELECT
    city,
    window_time,
    claim_count,
    total_claim_amount,
    avg_claim_amount,
    total_damage_assessed,
    CAST(ROUND(anomaly_result.forecast_value) AS BIGINT) AS expected_claim_amount,
    anomaly_result.upper_bound AS upper_bound,
    anomaly_result.lower_bound AS lower_bound,
    anomaly_result.is_anomaly AS is_anomaly
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = true
  AND total_claim_amount > anomaly_result.upper_bound;
```

**What it does:**
1. **Sets state TTL** to 14 days to prevent infinite state growth
2. **Aggregates** claims into 3-hour tumbling windows per city
3. **Applies ML_DETECT_ANOMALIES** using ARIMA time-series forecasting:
   - `minTrainingSize: 16` – Needs 2 days (16 windows) of baseline before detecting
   - `maxTrainingSize: 50` – Caps training data; prevents memory issues
   - `confidencePercentage: 95.0` – Detects significant deviations
   - `enableStl: FALSE` – No seasonal decomposition (disaster claims lack seasonality)
4. **Filters** to only anomalous spikes (above upper confidence bound)

**View the results:**

```sql
SELECT * FROM claims_anomalies_by_city;
```

![claims_anomalies_by_city](./assets/lab4/claims_anomalies_by_city.png)

---

### 2. Investigate Fraudulent Claims

Once anomalies are detected, use this query to create a table with claims from the anomaly window for investigation (limited to 10):

```sql
SET 'sql.state-ttl' = '14 d';

CREATE TABLE claims_to_investigate AS
SELECT
    c.claim_id,
    c.applicant_name,
    c.city,
    c.claim_narrative,
    c.claim_amount,
    c.damage_assessed,
    c.has_insurance,
    c.insurance_amount,
    c.is_primary_residence,
    c.assessment_date,
    c.disaster_date,
    c.assessment_source,
    c.shared_account,
    c.shared_phone,
    c.previous_claims_count,
    c.last_claim_date,
    c.claim_timestamp,
    a.window_time AS anomaly_window_time,
    a.total_claim_amount AS anomaly_total_amount,
    a.is_anomaly
FROM claims c
INNER JOIN claims_anomalies_by_city a
    ON c.city = a.city
    AND c.claim_timestamp >= a.window_time - INTERVAL '6' HOUR
    AND c.claim_timestamp <= a.window_time
WHERE c.claim_narrative <> ''
LIMIT 10;
```

This creates a table with 10 claims from the Naples anomaly window. Now query it to see what was flagged:

```sql
SELECT * FROM claims_to_investigate;
```

![claims_to_investigate](./assets/lab4/claims_to_investigate.png)

Next, we’ll **enrich each suspicious claim with FEMA IAPPG policy context** using vector search. For every claim in `claims_to_investigate`, we embed the claim narrative, search the `fema_policies_vectordb` for the three most relevant policy sections, and attach those chunks (plus titles, section references, fraud categories, and keywords) back onto the claim. This gives our downstream fraud detection agent concrete policy language to quote and cite when making its decision.

```sql
 SET 'sql.state-ttl' = '14 d';
                                    
  CREATE TABLE claims_to_investigate_with_policies AS                 
  WITH embedded AS (                                                   
      SELECT
          c.*,
          e.embedding AS narrative_embedding
      FROM claims_to_investigate c,
      LATERAL TABLE(ML_PREDICT('llm_embedding_model', c.claim_narrative)) e
  )
  SELECT
      c.claim_id,
      c.applicant_name,
      c.city,
      c.claim_amount,
      c.damage_assessed,
      c.has_insurance,
      c.insurance_amount,
      c.is_primary_residence,
      c.claim_narrative,
      c.assessment_date,
      c.disaster_date,
      c.assessment_source,
      c.shared_account,
      c.shared_phone,
      c.previous_claims_count,
      c.last_claim_date,
      c.claim_timestamp,
      c.anomaly_window_time,
      c.anomaly_total_amount,
      c.is_anomaly,
      vs.search_results[1].chunk AS policy_chunk_1,
      vs.search_results[1].score AS policy_score_1,
      vs.search_results[1].pages AS policy_pages_1,
      vs.search_results[1].section_reference AS policy_section_1,
      vs.search_results[1].title AS policy_title_1,
      vs.search_results[1].fraud_categories AS policy_fraud_cats_1,
      vs.search_results[1].policy_keywords AS policy_keywords_1,
      vs.search_results[2].chunk AS policy_chunk_2,
      vs.search_results[2].score AS policy_score_2,
      vs.search_results[2].pages AS policy_pages_2,
      vs.search_results[2].section_reference AS policy_section_2,
      vs.search_results[2].title AS policy_title_2,
      vs.search_results[2].fraud_categories AS policy_fraud_cats_2,
      vs.search_results[2].policy_keywords AS policy_keywords_2,
      vs.search_results[3].chunk AS policy_chunk_3,
      vs.search_results[3].score AS policy_score_3,
      vs.search_results[3].pages AS policy_pages_3,
      vs.search_results[3].section_reference AS policy_section_3,
      vs.search_results[3].title AS policy_title_3,
      vs.search_results[3].fraud_categories AS policy_fraud_cats_3,
      vs.search_results[3].policy_keywords AS policy_keywords_3
  FROM embedded c,
  LATERAL TABLE(
      VECTOR_SEARCH_AGG(
          fema_policies_vectordb,
          DESCRIPTOR(embedding),
          c.narrative_embedding,
          3
      )
  ) vs;
```

Then view the results:
```sql
SELECT * FROM `claims_to_investigate_with_policies`;
```

![claims_to_investigate_with_policies](./assets/lab4/claims_to_investigate_with_policies.png)

---

### 3. Define the Fraud Detection Agent

With each claim enriched with FEMA policy context, the next step is to define an AI agent that analyzes every claim holistically — checking the arithmetic, cross-referencing the narrative against structured fields, and citing specific FEMA IAPPG policy sections to justify each verdict.

Unlike the anomaly detection and RAG steps, which are pure SQL transforms, this agent performs multi-step reasoning: it evaluates each claim against a checklist of policy violations in sequence, weighs competing signals, and produces a structured verdict.

See [CREATE AGENT documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-agent.html).

```sql
CREATE AGENT `claims_fraud_investigation_agent`
USING MODEL `llm_textgen_model`
USING PROMPT 'OUTPUT RULES — read before anything else:
1. Respond with ONLY these four labeled sections, in this exact order:
   Verdict:
   Issues Found:
   Policy Basis:
   Summary:
2. NO markdown. No asterisks, no bold, no headers, no pound signs. Plain text only.
3. The Verdict line must contain exactly one word: APPROVE, APPROVE_PARTIAL, REQUEST_DOCS, DENY_INELIGIBLE, or DENY_FRAUD.

Correct format example:
Verdict: DENY_INELIGIBLE

Issues Found:
- Property is not a primary residence. Narrative states "our Naples beach house we rent out seasonally."
- is_primary_residence = "no" confirmed in structured data.

Policy Basis:
FEMA IAPPG Section 3 — IHP Housing Assistance is restricted to the applicants primary dwelling. Rental and vacation properties are categorically excluded.

Summary:
Claim denied. The property is a seasonal rental, not a primary residence, and is therefore ineligible for IHP assistance regardless of the damage amount.

---

You are a FEMA IHP fraud detection agent reviewing Hurricane Helene disaster assistance claims.

CHECKLIST — evaluate all nine in order:

1. CLAIM CEILING: Does claim_amount > damage_assessed? Auto-violation — FEMA cannot pay above verified assessed damage.
2. DUPLICATION OF BENEFITS: Does claim_amount > (damage_assessed minus insurance_amount)? FEMA covers only the uncompensated gap.
3. PRIMARY RESIDENCE: Is is_primary_residence = "no"? IHP covers primary dwellings only — vacation homes, investment properties, and rentals are ineligible.
4. ASSESSMENT SOURCE: Is assessment_source = "Self"? Self-assessments are not accepted. Flag if narrative claims a "FEMA inspection" but source = "Contractor".
5. PROPERTY USE (narrative): Does the narrative reveal a vacation rental, second home, or income-producing property?
6. INELIGIBLE ITEMS (narrative): Does the narrative claim pools, boats, landscaping, fences, outdoor furniture, or other non-essential property?
7. PRE-EXISTING DAMAGE (narrative): Does the narrative disclose wear, deterioration, or damage that predates the disaster?
8. EXPLICIT DUPLICATION (narrative): Does the narrative admit receiving insurance payment for the same damage while also claiming FEMA funds?
9. PRIOR CLAIMS: Does previous_claims_count > 0? Weight more heavily when combined with other violations.

VERDICTS:
- APPROVE: All checks pass, claim eligible as submitted.
- APPROVE_PARTIAL: Property is eligible and damage is verified, but the claim amount exceeds the allowable ceiling or includes specific ineligible items. Approve the eligible portion. State the calculated eligible amount in Issues Found (e.g., "Eligible amount: $45,000 (damage_assessed ceiling)").
- REQUEST_DOCS: No clear violation, but a determination requires additional documentation before a final decision.
- DENY_INELIGIBLE: Property or claim is categorically ineligible regardless of amount — non-primary residence declared in structured data, vacation rental or income-producing property revealed in narrative, or self-assessment with no third-party verification. No portion is approvable.
- DENY_FRAUD: Deliberate misrepresentation — false primary residence claim, explicit insurance duplication, or mathematically impossible amounts. Refer to OIG.

In Issues Found: cite dollar amounts and quote key phrases from the narrative. For APPROVE_PARTIAL, state the eligible amount. Write "None — claim passes all checks." if APPROVE.
In Policy Basis: cite specific section titles and references from the retrieved FEMA policy chunks.

REMINDER: Plain text only. No asterisks, no bold, no markdown of any kind.'
WITH (
  'max_iterations' = '10'
);
```

---

### 4. Run the Agent on All Flagged Claims

Now invoke the agent continuously against every claim in `claims_to_investigate_with_policies`. As each claim streams through, the agent receives the full structured data plus the three most relevant FEMA policy chunks, performs its checklist analysis, and writes a structured verdict to the output table.

The `REGEXP_EXTRACT` calls parse the four labeled sections from the agent's free-text response into typed columns.

See [AI_RUN_AGENT documentation](https://docs.confluent.io/cloud/current/flink/reference/functions/model-inference-functions.html#flink-sql-ai-run-agent-function).

```sql
SET 'sql.state-ttl' = '14 d';

CREATE TABLE claims_reviewed (
    PRIMARY KEY (claim_id) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    claim_id,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Verdict:\*{0,2}\s*([A-Z_]+)', 1)) AS verdict,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Summary:\*{0,2}\n([\s\S]+?)$', 1)) AS summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Issues Found:\*{0,2}\n([\s\S]+?)(?=\n\*{0,2}(?:Policy Basis|Summary|Verdict):|$)', 1)) AS issues_found,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), '\*{0,2}Policy Basis:\*{0,2}\n([\s\S]+?)(?=\n\*{0,2}(?:Summary|Verdict):|$)', 1)) AS policy_basis,
    applicant_name,
    claim_narrative,
    claim_amount,
    damage_assessed,
    insurance_amount,
    is_primary_residence,
    assessment_source,
    previous_claims_count,
    CAST(response AS STRING) AS raw_response
FROM claims_to_investigate_with_policies,
LATERAL TABLE(AI_RUN_AGENT(
    `claims_fraud_investigation_agent`,
    CONCAT(
        'CLAIM FOR REVIEW: ', claim_id, '\n',
        'Applicant: ', applicant_name, '\n',
        'Claim Amount: $', claim_amount, '\n',
        'Total Damage Assessed: $', COALESCE(CAST(damage_assessed AS STRING), '0'), '\n',
        'Insurance Payout: $', COALESCE(CAST(insurance_amount AS STRING), '0'), '\n',
        'Uncompensated Gap (damage minus insurance): $',
            CAST(CAST(
                COALESCE(CAST(damage_assessed AS DOUBLE), 0.0) -
                COALESCE(CAST(insurance_amount AS DOUBLE), 0.0)
            AS BIGINT) AS STRING), '\n',
        'Is Primary Residence: ', COALESCE(is_primary_residence, 'unknown'), '\n',
        'Assessment Source: ', COALESCE(assessment_source, 'unknown'), '\n',
        'Prior FEMA Claims: ', COALESCE(CAST(previous_claims_count AS STRING), '0'), '\n',
        '\nCLAIM NARRATIVE:\n', COALESCE(claim_narrative, '(none)'), '\n',
        '\nRETRIEVED FEMA POLICY SECTIONS:\n',
        '1. ', COALESCE(policy_title_1, 'N/A'), ' (', COALESCE(policy_section_1, 'N/A'), '):\n',
        COALESCE(policy_chunk_1, ''), '\n\n',
        '2. ', COALESCE(policy_title_2, 'N/A'), ' (', COALESCE(policy_section_2, 'N/A'), '):\n',
        COALESCE(policy_chunk_2, ''), '\n\n',
        '3. ', COALESCE(policy_title_3, 'N/A'), ' (', COALESCE(policy_section_3, 'N/A'), '):\n',
        COALESCE(policy_chunk_3, '')
    ),
    MAP['debug', 'true']
));
```

```sql
SELECT * FROM `claims_reviewed`;
```

![claims_reviewed](./assets/lab4/claims_reviewed.png)

## 5. Notify Claimants via Email with Streaming Agents

In this final step, you’ll extend the fraud‑detection pipeline with a **Streaming Agent** that emails a status update for a subset of reviewed claims.

- You will send emails for **only 3 reviewed claims** to limit LLM and Zapier usage.  
- For lab simplicity, **all emails go to a single recipient address** (your email), but:
  - Each email is addressed in the body to the individual claimant (`applicant_name`).
  - The message includes the **claim details** (amounts, etc.) and the **determination** (approved / partial / docs requested / denied).

---

### 5.1 Create the Zapier MCP Connection

First, tell Flink how to reach your Zapier MCP server by creating a **CONNECTION**.

> **Note for workshops:** Your instructor will provide a Zapier MCP token for you.  
> If you are running this lab on your own, follow the Zapier setup guide at [Zapier Setup](./assets/pre-setup/Zapier-Setup.md) in this project to obtain your token.

In the Flink SQL workspace for this environment/cluster, run: 
```sql
CREATE CONNECTION IF NOT EXISTS `zapier-mcp-connection`
WITH (
  'type' = 'MCP_SERVER',
  'endpoint' = 'https://mcp.zapier.com/api/v1/connect',
  'token' = '<YOUR_ZAPIER_TOKEN_HERE>',
  'transport-type' = 'STREAMABLE_HTTP'
);
```
- Replace `<YOUR_ZAPIER_TOKEN_HERE>` with your token.

### 5.2 Create the Zapier Tool (gmail_send_email)

Next, expose the Gmail tool from that connection as a Flink **TOOL**.
```sql
CREATE TOOL zapier
USING CONNECTION `zapier-mcp-connection`
WITH (
'type' = 'mcp',
'allowed_tools' = 'gmail_send_email',
'request_timeout' = '30'
);
```
### 5.3 Create the Claim Status Email Agent

Now define an agent that:

1. Reads `EMAIL RECIPIENT`, `EMAIL SUBJECT`, and `EMAIL BODY TEMPLATE` from its prompt.  
2. Calls **only** `gmail_send_email` using those exact values.  
3. Returns a brief confirmation string.

This agent uses the same `llm_textgen_model` you already used for the fraud‑investigation agent in this lab.
```sql
CREATE AGENT `claims_status_email_agent`
USING MODEL `llm_textgen_model`
USING PROMPT '
You are a FEMA claims notification assistant.

You will receive:
- A short description of a single reviewed claim (claimant, amounts, verdict).
- Email instructions in this exact format:

EMAIL RECIPIENT: <recipient email>
EMAIL SUBJECT: <email subject>
EMAIL BODY TEMPLATE: <full email body text>

Your tasks:

1. Parse the prompt and extract exactly:
   - EMAIL RECIPIENT
   - EMAIL SUBJECT
   - EMAIL BODY TEMPLATE

2. Call the gmail_send_email tool once with:
   - to: EMAIL RECIPIENT as a single string (NOT an array)
   - subject: EMAIL SUBJECT
   - body: EMAIL BODY TEMPLATE

3. Do not modify these values. Use them exactly as provided.

4. After calling gmail_send_email, return a brief text confirmation including:
   - The claim_id (if present)
   - The applicant_name (if present)
   - The verdict
   - The recipient email used

Do NOT call any tools other than gmail_send_email.
'
USING TOOLS `zapier`
WITH (
  'max_iterations' = '5'
);
```

---

### 5.4 Send Emails for 3 Reviewed Claims

Finally, invoke the email agent on **only 3 records** from `claims_reviewed` to reduce token and email usage.

> **IMPORTANT**  
> Replace `<<YOUR-EMAIL-ADDRESS-HERE>>` below with the **the email address** where you want to receive the notification emails.
```sql
SET 'sql.state-ttl' = '14 d';

CREATE TABLE claims_status_emails
WITH ('changelog.mode' = 'append') AS
SELECT
  r.claim_id,
  r.applicant_name,
  r.verdict,
  r.claim_amount,
  r.damage_assessed,
  r.insurance_amount,
  r.summary,
  r.issues_found,
  agent_result.status   AS agent_status,
  agent_result.response AS agent_response
FROM (
  -- Limit to 3 reviewed claims to reduce token and email usage
  SELECT *
  FROM claims_reviewed
  LIMIT 3
) r,
LATERAL TABLE (
  AI_RUN_AGENT(
    `claims_status_email_agent`,
    CONCAT(
      'Reviewed FEMA claim status notification.', '\n',
      'Claim ID: ', r.claim_id, '\n',
      'Applicant Name: ', COALESCE(r.applicant_name, 'Unknown'), '\n',
      'Verdict: ', COALESCE(r.verdict, 'UNKNOWN'), '\n',
      'Claim Amount: $', COALESCE(CAST(r.claim_amount AS STRING), '0'), '\n',
      'Damage Assessed: $', COALESCE(CAST(r.damage_assessed AS STRING), '0'), '\n',
      'Insurance Amount: $', COALESCE(CAST(r.insurance_amount AS STRING), '0'), '\n',
      'Issues Found: ', COALESCE(r.issues_found, 'None – claim passes all checks.'), '\n',
      'Summary: ', COALESCE(r.summary, 'No additional summary available.'), '\n',
      '\n',
      -- Email control instructions for the agent
      'EMAIL RECIPIENT: <<YOUR-EMAIL-ADDRESS-HERE>> ',
      'EMAIL SUBJECT: FEMA Disaster Assistance Claim Status – Claim ', r.claim_id,
      ' (', COALESCE(r.verdict, 'UNKNOWN'), ') ',
      'EMAIL BODY TEMPLATE: ',
      'Dear ', COALESCE(r.applicant_name, 'Applicant'), ',\n\n',
      'We are writing to inform you of the status of your FEMA disaster assistance claim.\n\n',
      'Claim details:\n',
      '- Claim ID: ', r.claim_id, '\n',
      '- City: (see claim record)\n',
      '- Claim Amount: $', COALESCE(CAST(r.claim_amount AS STRING), '0'), '\n',
      '- Verified Damage: $', COALESCE(CAST(r.damage_assessed AS STRING), '0'), '\n',
      '- Insurance Coverage: $', COALESCE(CAST(r.insurance_amount AS STRING), '0'), '\n\n',
      'Determination:\n',
      '- Decision: ', COALESCE(r.verdict, 'UNKNOWN'), '\n',
      '- Explanation: ', COALESCE(r.issues_found, 'None – your claim passed all checks.'), '\n\n',
      'This email was generated automatically by a streaming fraud detection and notification system built on Confluent Intelligence.\n\n',
      'Sincerely,\n',
      'FEMA Disaster Assistance Program'
    ),
    r.claim_id,
    MAP['debug', 'true']
  )
) AS agent_result(status, response);
```

Then you can verify that up to **3 emails** were triggered and see the agent responses:
```sql
SELECT * FROM claims_status_emails;
```

## Conclusion

By chaining these streaming components together, we've built an always-on, real-time fraud detection pipeline that:

1. **Detects** anomalous claim spikes in 6-hour windows across cities using `ML_DETECT_ANOMALIES`
2. **Isolates** the suspicious window and enriches every claim with relevant FEMA IAPPG policy sections using vector search
3. **Investigates** each claim autonomously using an AI agent that checks claim credibility, cross-references the claim narrative against structured fields, and cites specific policy violations
4. **Alerts** claimants in real time with individualized email status updates for reviewed claims, delivered through the same streaming agent pipeline.

The result is a deep, autonomous investigation of every flagged claim resulting in a proposed verdict — approve, request documentation, deny for policy violation, or deny for fraud — delivered in real time as claims arrive, with specific FEMA policy citations to support each decision.

---

## Troubleshooting

<details>
<summary>Click to expand</summary>

### No anomalies detected?

**Check:**
1. Re-publish : `uv run lab4_datagen`
2. Claims published to topic: `SELECT COUNT(*) FROM claims;` (should be ~36,000)
3. Wait for baseline: ARIMA needs 16 windows (2 days with 3-hour windows) before detecting

The anomaly should appear after data publishing completes and Flink processes all windows up to Day 3.

### Error: Table `claims_anomalies_by_city` does not exist?

**Solution:**

Run the CREATE TABLE query in the Flink UI (see Step 1 above). The anomaly detection table is created manually, not via Terraform.

### Query returns 0 rows?

**Check:**
1. Terraform deployed: `terraform output` should show table IDs
2. Data exists: `SELECT * FROM claims LIMIT 10;`
3. Wait for processing: Flink may take 1-2 minutes to process all windows

### Too many or too few anomalies?

The ARIMA parameters are tuned to detect exactly 1 anomaly (Naples, Day 3).

If you see different results:
- **0 anomalies:** Decrease `confidencePercentage` to 90.0
- **>3 anomalies:** Increase `confidencePercentage` to 99.0
- **Wrong timing:** Re-publish data: `uv run lab4_datagen`

</details>

---

## 🧹 Clean-up

When you're done with the lab:

```bash
uv run destroy
```

Choose your cloud provider when prompted to remove all lab-related resources.

---

## Navigation

- **← Back to Overview**: [Main README](./README.md)
- **← Previous Lab**: [Lab3: Agentic Fleet Management](./LAB3-Walkthrough.md)
- **🧹 Cleanup**: [Cleanup Instructions](./README.md#cleanup)
