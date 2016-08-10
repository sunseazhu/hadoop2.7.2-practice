/**
 * 这个主要是统计一个流量统计文件中的同一个号码的上行流量、下行流量、总流量之和。
 * 文件存储在data文件夹中，运行时，文件时上传到hdfs中的，文件的大致格式如下：
 * 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
 * 1363157991076 	13926435656	20-10-7A-28-CC-0A:CMCC	120.196.100.99			2	4	132	1512	200
 * 
 */
/**
 *<p>
 *	Description: 
 *<p>
 *	Company: nupt
 *
 *	@author zhuxy
 *	2016年8月10日 下午5:18:14
 */
package cn.edu.nupt.hadoop.mr.flowsum;