<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme()+"://"+request.getServerName()
            +":"+request.getServerPort()+path+"/";
%>
<html>
<head>
    <base href=" <%=basePath%>">
</head>
<script src="lib/jquery/jquery-1.7.js" type="text/javascript"></script>
<script>

    function query(){
        var startTime = $("#startTime").val();
        var endTime = $("#endTime").val();
        var nation = $("#nation").val();
        var pid = $("#pid").val();
        console.info(startTime + " : " + endTime + " : " + nation + " : " + pid)

        $.ajax({
            type: 'POST',
            url:"ad",
            data:'action=hit&startTime=' + startTime + "&endTime=" + endTime + "&nation=" + nation + "&pid=" + pid,
            success: function(msg){
                $("#content").html(msg + "<br/>");
            }
        });
    }

</script>

<body>
    开始时间：<input name="startTime" id="startTime" />
    结束时间：<input name="endTime" id="endTime" />
    Nation: <input name="nation" id="nation"/>
    项目名：<input name="pid" id="pid"/>
    <input type="button" value="查询" onclick="query()"/> <br/>

    <div id="content" style="height:200px;width: 400px;background: #000000;color: #ffffff" >

    </div>

</body>
</html>