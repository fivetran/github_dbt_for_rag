WITH assignee_groups AS (
    SELECT 
        ia.issue_id,
        LISTAGG(u.login, ', ') WITHIN GROUP (ORDER BY u.login) AS assignees
    FROM {{ source('github','issue_assignee') }} ia
    LEFT JOIN {{ source('github','user') }} u ON ia.user_id = u.id
    GROUP BY ia.issue_id
),
label_groups AS (
    SELECT 
        il.issue_id,
        LISTAGG(l.name, ', ') WITHIN GROUP (ORDER BY l.name) AS labels
    FROM {{ source('github','issue_label') }} il
    LEFT JOIN {{ source('github','label') }} l ON il.label_id = l.id
    GROUP BY il.issue_id
),
issue_details AS (
    SELECT
        i.id,
        i.created_at,
        i.updated_at,
        i.number,
        i.state,
        i.title,
        i.body, 
        u.login,
        COALESCE(ag.assignees, 'No one assigned') as assignees,
        COALESCE(lg.labels, 'None yet') as labels
    FROM {{ source('github','issue') }} i
    LEFT JOIN {{ source('github','user') }} u ON i.user_id = u.id
    LEFT JOIN assignee_groups ag ON i.id = ag.issue_id
    LEFT JOIN label_groups lg on i.id = lg.issue_id
    WHERE NOT i.pull_request 
)
SELECT
    id,
    CONCAT(
        '# ', title, ' (#', number, ')\n\n',
        'Opened by ', login, ' on ', created_at, '\n',
        'Assigned to : ', assignees, '\n',
        'Labels : ', labels, '\n',
        'Last updated on ', updated_at, '\n',
        'Issue is currently ', state, '\n\n',
        '## DESCRIPTION : \n', body, '\n'
    ) AS content
FROM issue_details