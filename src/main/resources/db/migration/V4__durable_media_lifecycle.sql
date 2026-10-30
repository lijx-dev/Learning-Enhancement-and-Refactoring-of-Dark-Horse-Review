ALTER TABLE tb_media_owner
  ADD COLUMN state VARCHAR(16) NOT NULL DEFAULT 'ACTIVE';
ALTER TABLE tb_business_outbox
  ADD COLUMN resource_path VARCHAR(255) NULL;

CREATE TABLE tb_blog_media (
  blog_id BIGINT NOT NULL,
  path VARCHAR(255) NOT NULL,
  PRIMARY KEY (blog_id,path),
  KEY idx_media_reference (path,blog_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Backfill previously published, owned images in both default-frontend and raw upload formats.
INSERT IGNORE INTO tb_blog_media(blog_id,path)
SELECT links.blog_id, owner.path FROM (
  SELECT b.id AS blog_id,
    SUBSTRING_INDEX(SUBSTRING_INDEX(b.images,',',positions.n),',',-1) AS image_path
  FROM tb_blog b JOIN (
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
  ) positions ON positions.n <= 1+CHAR_LENGTH(b.images)-CHAR_LENGTH(REPLACE(b.images,',',''))
  WHERE b.images <> ''
) links JOIN tb_media_owner owner
  ON BINARY owner.path = BINARY (CASE WHEN links.image_path LIKE '/imgs/blogs/%'
    THEN SUBSTRING(links.image_path,6) ELSE links.image_path END);
