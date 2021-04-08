package atguigu.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();

    @Override
    //key 偏移量
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取一行数据，转成字符串
        String line = value.toString();
        //2. 切割数据
        String[] split = line.split("\t");
        //3. 抓取需要的数据：手机号，上行流量，下行流量
        String phone = split[0];
        String up = split[1];
        String down = split[2];

        //4. 封装 outK outV
        outV.set(phone);
        outK.setUpFlow(Long.parseLong(up));
        outK.setDownFlow(Long.parseLong(down));
        outK.setSumFlow();

        context.write(outK, outV);
    }
}
