package cn.edu.nupt.hadoop.mr.flowsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.sun.javadoc.ThrowsTag;
import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;

public class FlowBean implements WritableComparable {
	
	private String phone_NB;
	

	public String getPhone_NB() {
		return phone_NB;
	}

	public void setPhone_NB(String phone_NB) {
		this.phone_NB = phone_NB;
	}

	public int getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(int up_flow) {
		this.up_flow = up_flow;
	}

	public int getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(int down_flow) {
		this.down_flow = down_flow;
	}

	public int getUp_down_flow() {
		return up_down_flow;
	}

	public void setUp_down_flow(int up_down_flow) {
		this.up_down_flow = up_down_flow;
	}

	private int up_flow;
	private int down_flow;
	private int up_down_flow; 
	

	/**
	 * 这个方法主要用于反射机制。
	 * 反射机制：
	 * （1）当对象转换成流在网络中传输的时候，需要将对象转换成流。此时不使用无参的构造函数。
	 * （2）当从流中读取数据，需要将流读出的数据反射成对象。此时必须要使用无参的构造函数。
	 */
	public FlowBean() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * 用于属性值赋值
	 * @param phone_NB
	 * @param up_flow
	 * @param down_flow
	 * @param up_down_flow
	 */
	public FlowBean(String phone_NB, int up_flow, int down_flow, int up_down_flow) {
		super();
		this.phone_NB = phone_NB;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.up_down_flow = up_down_flow;
	}
	
	/**
	 * 格式化顺序写入，也需要顺序读出
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(phone_NB);
		out.writeInt(up_flow);
		out.writeInt(down_flow);
		out.writeInt(up_down_flow);
	}

	/**
	 * 此处的读出需要和写入的顺序一致，并且格式也需要一致。
	 * 对于流来说就是二进制，没格式，但是会根据读的时候规定的格式进行解析。因此需要和写入的时候一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.phone_NB=in.readUTF();
		this.up_flow=in.readInt();
		this.down_flow=in.readInt();
		this.up_down_flow=in.readInt();
	}
	@Override
	public String toString() {
		return "\t"+this.up_flow+"\t"+this.down_flow+"\t"+this.up_down_flow;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if(o instanceof FlowBean){
			return ((FlowBean) o).up_down_flow>this.up_down_flow?1:-1;
		}
		// 此处写法有点小问题，暂时先这么写着
		else{
			return 0;
		}
		
	}
}
