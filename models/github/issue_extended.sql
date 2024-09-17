{{ config(
    constraints={
        'primary_key': ['id']
    },
    post_hook=[
      "ALTER TABLE {{ this }} ADD PRIMARY KEY (id)"
    ]
) }}

SELECT 
    i.id,
    i.created_at,
    i.updated_at,
    i.number,
    i.state,
    i.title,
    COALESCE(i.body, 'UNKNOWN') AS body,
    i.locked,
    COALESCE(i.closed_at, CURRENT_TIMESTAMP()) AS closed_at,
    i.repository_id,
    COALESCE(i.milestone_id, -1) AS milestone_id,
    i.pull_request,
    i.user_id,
    i._fivetran_synced,
    COALESCE(id.content, 'UNKNOWN') AS original_text,
    CONCAT('https://github.com/', r.full_name, '/issues/', i.number) as url
FROM {{  source('github', 'issue') }} i 
JOIN {{ ref('issue_document') }} id ON i.id = id.id
JOIN {{ source('github', 'repository') }} r ON i.repository_id = r.id