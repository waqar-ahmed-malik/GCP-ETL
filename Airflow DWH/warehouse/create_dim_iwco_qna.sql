CREATE VIEW IF NOT EXISTS `usps-bigquery.data_warehouse.dim_iwco_qna`
AS
SELECT
    mac_id,
    campaign_survey_response_id,
    campaign_response_id,
    survey_question_id,
    question_text,
    survey_answer_id,
    answer_text
FROM 
    `usps-bigquery.operational.iwco_qna`