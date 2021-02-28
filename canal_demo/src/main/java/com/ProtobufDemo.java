package com;

import com.google.protobuf.InvalidProtocolBufferException;
import com.zhao.protobuf.DemoModel;

//使用protobuf进行数据的序列化和反序列化
public class ProtobufDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();
        builder.setName("zhaochong");
        builder.setId(1);
        builder.setSex("男");

        //获取user对象的属性
        DemoModel.User build = builder.build();
        System.out.println(build.getId());
        System.out.println(build.getName());
        System.out.println(build.getSex());

        //数据序列化与反序列化
        byte[] bytes = builder.build().toByteArray();
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }
        //反序列化
        DemoModel.User user = DemoModel.User.parseFrom(bytes);
        System.out.println(user.getName());
    }
}
