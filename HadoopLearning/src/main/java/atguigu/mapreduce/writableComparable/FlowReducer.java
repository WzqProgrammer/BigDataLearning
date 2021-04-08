package atguigu.mapreduce.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //循环遍历写出，避免总流量相同的情况
        for (Text value : values) {
            //将k，v对调写出
            context.write(value, key);
        }
    }
}
