package atguigu.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partition=0;
        String prePhone = text.toString().substring(0,3);

        if("136".equals(prePhone)){
            partition=0;
        }
        else if("137".equals(prePhone)){
            partition=1;
        }
        else if("138".equals(prePhone)){
            partition=2;
        }
        else if("139".equals(prePhone)){
            partition=3;
        }
        else{
            partition=4;
        }

        return partition;
    }
}
