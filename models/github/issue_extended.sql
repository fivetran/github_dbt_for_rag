SELECT 
    i.* ,
    id.content AS original_text
FROM {{  source('github', 'issue') }} i 
JOIN {{ ref('issue_document') }} id ON i.id = id.id