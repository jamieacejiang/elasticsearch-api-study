package com.ctwom.utils;

import com.ctwom.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Component
public class HtmlParseUtil {
//    public static void main(String[] args) throws IOException {
//        new HtmlParseUtil().parseJD("java").forEach(System.out::println);
//    }

    public List<Content> parseJD(String keywords) throws IOException {
        ArrayList<Content> list = new ArrayList<>();

        //获得请求 https://search.jd.com/Search?keyword=java
        String url = "https://search.jd.com/Search?keyword=" + keywords;
        //解析网页 jsoup返回的Document对象就是浏览器的Document就是页面对象
        Document document = Jsoup.parse(new URL(url), 30000);
        //所有你在js中可以使用的方法，这里都能用
        Element element = document.getElementById("J_goodsList");

        //System.out.println(element.html());

        //找到所有的ul-li元素
        Elements lis = element.getElementsByTag("li");
        //获取元素中的内容
        for (Element li : lis) {
            //关于这种图片特别多的网站，所有的图片都是延迟加载的
            String img = li.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = li.getElementsByClass("p-price").eq(0).text();
            String title = li.getElementsByClass("p-name").eq(0).text();
            Content content = new Content();
            content.setImg(img);
            content.setTitle(title);
            content.setPrice(price);
            list.add(content);
        }
        return list;
    }
}
