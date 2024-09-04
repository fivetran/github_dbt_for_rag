WITH details AS (
    SELECT
        ic.id,
        ic.issue_id,
        ic.created_at,
        ic.body,
        u.login,
        CASE
            WHEN u.type = 'Bot' THEN 'bot'
            WHEN u.type = 'User' AND u.id = i.user_id THEN 'author'
            ELSE 'contributor'
        END AS role
    FROM {{ source('github', 'issue_comment') }} ic
    JOIN {{ source('github', 'user') }} u ON ic.user_id = u.id
    JOIN {{ source('github', 'issue') }} i ON ic.issue_id = i.id
)
SELECT 
    id,
    issue_id,
    created_at,
    CONCAT(
        '## ', login, ' (', role, ') commented on ', created_at, '\n',
        body
    ) AS content
FROM details