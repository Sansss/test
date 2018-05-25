package com.rtdb.test;

import java.io.BufferedReader;
import java.io.FileReader;

public class BaseTest {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		   try {    
			   Long sum =0l;
	            BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\AppData\\Local\\Programs\\Python\\Python36\\123.csv"));//换成你的文件名   
	            reader.readLine();//第一行信息，为标题信息，不用,如果需要，注释掉   
	            String line = null;    
	            while((line=reader.readLine())!=null){    
	                String item[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分   
	                String last = item[1];//这就是你要的数据了   
	                //int value = Integer.parseInt(last);//如果是数值，可以转化为数值   
	                sum=sum+Long.parseLong(last);
	                System.out.println(last);    
	            }    
	        } catch (Exception e) {    
	            e.printStackTrace();    
	        }    
	    }    
	}

