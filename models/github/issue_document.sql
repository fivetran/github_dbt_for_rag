SELECT 
    ic.id,
    CONCAT(
        ic.content, 
        '\n\n---\n\n'
        LISTAGG(icc.content , '\n\n---\n\n') WITHIN GROUP (ORDER BY icc.created_at)
    ) AS content
FROM {{ ref('issue_concatenated') }} ic
JOIN {{ ref('issue_comment_concatenated') }} icc ON ic.id = icc.issue_id
GROUP BY ic.id