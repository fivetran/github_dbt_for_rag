SELECT 
    i.* ,
    ic.issue_content AS original_text
FROM {{  source('github', 'issue') }} i 
JOIN {{ ref('issue_concatenated') }} ic ON i.id = ic.id