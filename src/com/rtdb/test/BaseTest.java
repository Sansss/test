package com.rtdb.test;

import java.io.BufferedReader;
import java.io.FileReader;

public class BaseTest {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		   try {    
			   Long sum =0l;
	            BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Administrator\\AppData\\Local\\Programs\\Python\\Python36\\123.csv"));//��������ļ���   
	            reader.readLine();//��һ����Ϣ��Ϊ������Ϣ������,�����Ҫ��ע�͵�   
	            String line = null;    
	            while((line=reader.readLine())!=null){    
	                String item[] = line.split(",");//CSV��ʽ�ļ�Ϊ���ŷָ����ļ���������ݶ����з�   
	                String last = item[1];//�������Ҫ��������   
	                //int value = Integer.parseInt(last);//�������ֵ������ת��Ϊ��ֵ   
	                sum=sum+Long.parseLong(last);
	                System.out.println(last);    
	            }    
	        } catch (Exception e) {    
	            e.printStackTrace();    
	        }    
	    }    
	}

