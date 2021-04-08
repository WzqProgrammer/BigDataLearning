package atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        //用户
        String user = "atguigu";

        //1. 获取到客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //3. 关闭资源
        fs.close();
    }

    //创建文件
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {

        //2. 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan_2"));
    }

    //上传
    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false, false,
                new Path("E:\\Test/hello.txt"), new Path("hdfs://hadoop102:8020/xiyou"));
    }

    //下载文件
    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102:8020/xiyou/hello.txt"),
                new Path("E:\\Test"), true);
    }

    //文件更名和移动
    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("hdfs://hadoop102:8020/xiyou/hello.txt"),
                new Path("hdfs://hadoop102:8020/xiyou/world.txt"));
    }

    //删除文件和目录
    @Test
    public void testDelete() throws IOException {
        fs.delete(new Path("/xiyou"), true);
    }

    //文件详情查看
    @Test
    public void testListFilesInfo() throws IOException {
        RemoteIterator<LocatedFileStatus> listFilesInfo = fs.listFiles(new Path("/"), true);
        while(listFilesInfo.hasNext()){
            LocatedFileStatus fileStatus = listFilesInfo.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //文件和文件夹的判断
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus: fileStatuses){
            if(fileStatus.isFile()){
                System.out.println("f:" + fileStatus.getPath().getName());
            }
            else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }
}
