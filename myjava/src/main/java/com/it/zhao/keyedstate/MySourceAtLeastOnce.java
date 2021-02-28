package com.it.zhao.keyedstate;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.io.RandomAccessFile;

public class MySourceAtLeastOnce extends RichParallelSourceFunction<String> implements CheckpointedFunction {
    private String path;

    public MySourceAtLeastOnce(String path) {
        this.path = path;
    }

    //设置初始偏移量
    private Long offset = 0L;
    private transient ListState<Long> listState;
    private boolean flag = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int index = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile rw = new RandomAccessFile(path + "\\" + index + ".txt", "r");
        rw.seek(offset);
        while (flag) {
            String line = rw.readLine();
            if (line != null) {
                line = new String(line.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8);
                //加锁，等checkpoint完成之后进行读取数据
                synchronized (sourceContext.getCheckpointLock()) {
                    //把文件指针放到最前面
                    offset = rw.getFilePointer();
                    sourceContext.collect(index + ".txt :" + line);
                }
            } else {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }


    //快照方法，定期更新
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(offset);
    }

    //初始化状态偏移量
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Long> offsetDescriptor = new ListStateDescriptor<>("offset", Long.class);
        listState = context.getOperatorStateStore().getListState(offsetDescriptor);
        if (context.isRestored()) {
            Iterable<Long> longs = listState.get();
            for (Long aLong : longs) {
                offset = aLong;
            }
        }
    }


}
