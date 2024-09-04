with issue_comments_collected AS (
    SELECT 
        issue_id,
        LISTAGG(content , '\n\n\n') WITHIN GROUP (ORDER BY created_at) as content
    FROM {{ ref('issue_comment_concatenated') }}
    GROUP BY issue_id
)
SELECT 
    ic.id,
    CONCAT(
        ic.content, 
        '\n\n\n',
        icc.content
    ) AS content
FROM {{ ref('issue_concatenated') }} ic
JOIN issue_comments_collected icc ON ic.id = icc.issue_id