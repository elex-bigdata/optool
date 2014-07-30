<%@ page language="java" pageEncoding="UTF-8"%>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://"
			+ request.getServerName() + ":" + request.getServerPort()
			+ path + "/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
	<head>
		<base href="<%=basePath%>">

		<title>常用工具</title>

		<meta http-equiv="pragma" content="no-cache">
		<meta http-equiv="cache-control" content="no-cache">
		<meta http-equiv="expires" content="0">
		<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
		<meta http-equiv="description" content="This is my page">

		<link href="lib/css/ligerui-common-extend.css" rel="stylesheet" type="text/css" />
        <script src="lib/jquery/jquery-1.7.js" type="text/javascript"></script>
		
		<style type="text/css">
		div{float: left; width: 45%;}
		div ul{list-style: none; margin-top: 10px;}
		div ul li{height: 25px;}
		</style>
	</head>

	<body style="padding: 20px 0 0 20px;">
		<div>
			<p>时间转换</p>
			<input type="text" id="time" />
			<input type="button" class="l-button" value="转换" onclick="convertTime()" />
			<ul id="times"></ul>
		</div>
		

		
	<script type="text/javascript">
    function convertTime(){
    	var time = $('#time').val();
    	if(time == null || time == ''){
    		alert("请输入需转换的时间");
    		return;
    	}
    	var url = 'op?action=convertTime&time='+time;
	   	$.ajax({
	   		async: false,
	   		url: url,
	   		type: 'post',
	   		dataType: 'json',
	   		success:function(data){
		    	if(data.success){
			    	$('#times').append('<li>' + data.result + '</li>');
		    	}else{
		    		alert(data.msg);
		    	}
	   		}
	   	});
    }
    function convertIp(){
    	var ip = $('#ip').val();
    	if(ip == null || ip == ''){
    		alert("请输入需转换的IP");
    		return;
    	}
    	var url = 'op?action=convertIp&ip='+ip;
	   	$.ajax({
	   		async: false,
	   		url: url,
	   		type: 'post',
	   		dataType: 'json',
	   		success:function(data){
		    	if(data.success){
			    	$('#ips').append('<li>' + data.result + '</li>');
		    	}else{
		    		alert(data.msg);
		    	}
	   		}
	   	});
    }
    </script>
	</body>
</html>
