# 可选初始图片

本仓库不依赖外部的课程 Nginx 目录，也不打包本机用户上传的文件。

开发环境将新上传的图片持久保存在 Docker `uploads` 卷；缺失的 `/imgs/` 图片由 Nginx 返回前端占位图。SQL 中的第三方图片 URL 仍依赖外站，不保证始终可用。

如持有可再分发的原始演示图片，可将它们放在自己的目录，并执行：

```bash
python deploy/package-release.py --images-dir /path/to/demo-images
```

发布包只复制支持的图片文件。不要将真实用户照片、身份证件或其他个人文件作为演示素材提交到仓库。
