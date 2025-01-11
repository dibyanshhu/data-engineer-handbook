# Week 6 Data Pipeline Maintenance

## Business Areas and Pipelines:

The five pipelines that need to be managed focus on the following areas:

### Profit:

     - Unit-level profit needed for experiments.
    - Aggregate profit reported to investors.

### Growth:

     - Aggregate growth reported to investors.
    - Daily growth needed for experiments.

### Engagement:

     - Aggregate engagement reported to investors.

## Ownership Structure

### Primary Owners:

 Primary owners will be responsible for the day-to-day management of the pipelines, ensuring data quality, resolving issues, and making enhancements. They will be experts on the data and processes for each pipeline.

    - Profit Pipelines: Data Engineer specializing in financial data or business intelligence.
    - Growth Pipelines - Primary Owner: Data Engineer with a focus on user growth or marketing metrics.
    - Engagement Pipelines - Primary Owner: Data Engineer focused on user engagement and interaction data.

### Secondary Owners:

Secondary owners act as backups and will help with any escalated issues or tasks when the primary owner is unavailable. They will have a general understanding of the pipeline and processes but will be less involved in day-to-day management.
    - Profit Pipelines - Secondary Owner: Data Engineer or analyst familiar with the business’s financial data processes.
    - Growth Pipelines - Secondary Owner: Data Engineer or data analyst with knowledge in growth and user acquisition metrics.
    - Engagement Pipelines - Secondary Owner: Data Engineer or data analyst who understands engagement metrics and user activity.

### On-Call Schedule

#### Work Week:

    - The on-call schedule should rotate weekly, with one data engineer from each pipeline being on-call for a week at a time. Each engineer will handle urgent issues, troubleshooting, and support during their on-call period.

### Holiday Coverage:

To ensure coverage during holidays, on-call engineers will be scheduled with two engineers available for high-priority issues over major holidays. These schedules should be communicated and planned well in advance (e.g., the week before the holiday).

### On-Call Rotation Example:

    - Week 1: Engineer A (Profit), Engineer B (Growth), Engineer C (Engagement)
    - Week 2: Engineer B (Profit), Engineer C (Growth), Engineer D (Engagement)
    - Week 3: Engineer C (Profit), Engineer D (Growth), Engineer A (Engagement)
    -  Rotate every week with holiday shifts planned well in advance.

#### Runbooks for Reporting Pipelines

    - General Structure for Each Pipeline
    - Each pipeline will have a runbook that outlines the essential steps to monitor, maintain, and troubleshoot it. The runbook should include:

#### Pipeline Overview:

High-level description of the pipeline’s purpose and scope, key stakeholders, and technologies used (e.g., ADF, Databricks, SQL, etc.).

#### Metrics Monitored:

A list of the key performance indicators (KPIs) tracked by the pipeline, along with their definitions (e.g., daily growth, unit-level profit).

#### Common Issues and Potential Failures:

Possible points of failure in the pipeline, such as:
    - Data source unavailability (e.g., missing data from a source or unresponsive API).
    - Pipeline jobs failing due to resource limits (e.g., timeouts or memory overload).
    - Data quality issues (e.g., duplicates, missing values, or incorrect transformations).
    - Aggregation or computation errors (e.g., mismatched data or incorrect joins).
    - Scheduling or timing issues (e.g., delayed jobs affecting downstream processes).
    - Error Handling and Notification Procedures:
    - Procedures for escalating errors, including notification channels (e.g., Slack, email, SMS).  - - This section will also define the level of severity for different types of failures.

#### Escalation Pathways:

Clearly defined steps to escalate issues to secondary owners or other teams when the primary owner is unavailable.

####  Sample Pipeline: Profit Pipeline (Aggregate Profit)

##### Pipeline Overview:

This pipeline aggregates profit data from multiple sources to report to investors.

###### Key technologies: Azure Data Factory, Databricks, SQL-based transformations.

###### Metrics Monitored:

    - Aggregate profit: Sum of profits across all products, regions, and time periods.
    - Unit-level profit: Profit at the granularity of each product or service.
    - Common Issues and Potential Failures:
    - Data Source Failures: Data from the product database is missing or incomplete.
    - Transformation Failures: Errors in the SQL transformations that calculate profit due to data schema changes.
    - Performance Issues: Slow performance due to large data volumes or resource limitations on Databricks clusters.
    - Error Handling and Notification Procedures:
    - Notifications via Slack and email when a pipeline fails.

##### High-priority issues (e.g., data missing for investor reports) are escalated to secondary owners and managers.

###### Escalation Pathways:

    - If the primary owner cannot resolve the issue within 30 minutes, the secondary owner takes over.
For major incidents (e.g., data loss), escalate to the team lead.