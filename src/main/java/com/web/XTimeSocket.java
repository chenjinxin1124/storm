package com.web;

import com.service.ShopService;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;

/**
 * Created by Administrator on 2018/6/23.
 */
@ServerEndpoint("/web_socket/x_time")
public class XTimeSocket {

    @OnMessage
    public void onMessage(String message, Session session)
            throws IOException, InterruptedException {

        while(true){
            String returnStr = String.valueOf(ShopService.getXtime_amt());
            System.out.println("x_time>>>>"+returnStr);

            session.getBasicRemote().sendText(returnStr);
            Thread.sleep(1000);

        }
    }

    @OnError
    public void onError(Session session, Throwable error){
        System.out.println("发生错误");
        error.printStackTrace();
    }



    @OnOpen
    public void onOpen () {
        System.out.println("连接到了服务了********** Client connected");
    }
    @OnClose
    public void onClose () {
        System.out.println("Connection closed");
    }

}
