.gitignore只能忽略那些原来没有被track的文件，如果某些文件已经被纳入了版本管理中，则修改.gitignore是无效的。那么解决方法就是先把本地缓存删除（改变成未track状态），然后再提交：
    输入：
    git rm -r –cached filePath
    git commit -m “remove xx”
    或者：
    git rm -r –cached .
    git add .
    git commit -m “update .gitignore”

来解释下几个参数 -r 是删除文件夹及其子目录 –cached 是删除暂存区里的文件而不删除工作区里的文件，第一种是删除某个文件，第二种方法就把所有暂存区里的文件删了，再加一遍，相当于更新了一遍。

---------------------

本文来自 这很自然 的CSDN 博客 ，全文地址请点击：https://blog.csdn.net/qq_34590097/article/details/56284935?utm_source=copy