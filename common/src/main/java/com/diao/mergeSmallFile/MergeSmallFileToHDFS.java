package com.diao.mergeSmallFile;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

/**
 * @author: Chenzhidiao
 * @date: 2020/3/16 16:15
 * @description:合并小文件至HDFS
 * @version: 1.0
 */
public class MergeSmallFileToHDFS {
    private static FileSystem fs = null;
    private static FileSystem local = null;

    public static void main(String[] args) throws IOException, URISyntaxException {
        list();
    }

    public static void list() throws IOException, URISyntaxException {
        // 读取hadoop文件系统的配置
        Configuration conf = new Configuration();
        //文件系统访问接口
        URI uri = new URI("hdfs://hadoop100:9000");
        //创建FileSystem对象fs
        fs = FileSystem.get(uri, conf);//get拿值
        // 获得本地文件系统
        local = FileSystem.getLocal(conf);//get拿值
        //过滤目录下的 svn 文件
        FileStatus[] dirstatus = local.globStatus(new Path("F:/mergeSmallFile/*"), new RegexExcludePathFilter("^.*svn$"));
        //^表示匹配我们字符串开始的位置    .*代表匹配任意字符任意多个	$代表以svn结尾
        //RegexExcludePathFilter来只排除我们不需要的，即svn格式
        //获取mergeSmallFile目录下的所有文件路径
        Path[] dirs = FileUtil.stat2Paths(dirstatus);
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        for (Path dir : dirs) {//for循环，即将dirs的值一一传给Path dir
            String fileName = dir.getName().replace("-", "");//文件名称
            //只接受日期目录下的.txt文件
            FileStatus[] localStatus = local.globStatus(new Path(dir + "/*"), new RegexAcceptPathFilter("^.*txt$"));
            //RegexAcceptPathFilter来只接收我们需要，即txt格式

            // 获得日期目录下的所有文件
            Path[] listedPaths = FileUtil.stat2Paths(localStatus);
            //输出路径
            Path block = new Path("hdfs://hadoop100:9000/outData/MergeSmallFilesToHDFS/" + fileName );
            // 打开输出流
            out = fs.create(block);
            for (Path p : listedPaths) {//for循环，即将listedPaths的值一一传给Path p
                in = local.open(p);// 打开输入流
                IOUtils.copyBytes(in, out, 4096, false); // 复制数据

            //IOUtils类的copyBytes将hdfs数据流拷贝到标准输出流System.out中，
            //copyBytes前两个参数好理解，一个输入，一个输出，第三个是缓存大小，第四个指定拷贝完毕后是否关闭流。
            //要设置为false，标准输出流不关闭，我们要手动关闭输入流。即，设置为false表示关闭输入流

                // 关闭输入流
                in.close();
            }
            if (out != null) {
                // 关闭输出流a
                out.close();
            }
        }

    }

    /**
     * @function 过滤 regex 格式的文件
     */
    public static class RegexExcludePathFilter implements PathFilter {
        private final String regex;

        public RegexExcludePathFilter(String regex) {
            this.regex = regex;
        }

        public boolean accept(Path path) {
            // TODO Auto-generated method stub
            boolean flag = path.toString().matches(regex);
            return !flag;
        }

    }

    /**
     * @function 接受 regex 格式的文件
     */
    public static class RegexAcceptPathFilter implements PathFilter {
        private final String regex;

        public RegexAcceptPathFilter(String regex) {
            this.regex = regex;
        }

        public boolean accept(Path path) {
            // TODO Auto-generated method stub
            boolean flag = path.toString().matches(regex);
            return flag;
        }

    }
}
