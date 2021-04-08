package atguigu.mapreduce.writableComparable;

import lombok.Getter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//1. 继承Writable接口
@Getter
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;   //上行流量
    private long downFlow;    //下行流量
    private long sumFlow;     //总流量

    //2. 提供无参构造函数
    public FlowBean(){
    }
    //3. 提供三个参数的getter和setter方法
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    //4.  实现序列化和反序列化方法,注意顺序一定要保持一致
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    //5. 重写ToString
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }


    @Override
    public int compareTo(FlowBean o) {
        if(this.sumFlow > o.sumFlow){
            return -1;
        }
        else if(this.sumFlow < o.sumFlow){
            return 1;
        }
        return 0;
    }
}
