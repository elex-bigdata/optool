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
<script src="lib/jquery/jquery-dateFormat.js" type="text/javascript"></script>
<script src="lib/calendar/lhgcalendar.min.js" type="text/javascript"></script>
<script>

    $(function ()
    {
        loadAds();

    })


    function query(){

    }

    function loadAds(){
        $.ajax({
            url: 'ad',
            data: {"action":'ads'},
            type: 'post',
            dataType: 'json',
            success:function(data){
                $('#ads').empty();
                for(var k in data){
                    $('#ads').append("<option value='"+ k +"'>" + data[k] +"</option>");
                }
                $('#ads').on('change', function(e){

                });
                $('#ads').change(function(e){
                    var id = $("#ads").val();
                    console.info("change " + id );

                    $('#ad').attr("src","ad?action=adtest&id="+id);
                });
                $('#ads').live("change", function(e){
                    console.info(" live ")
                });
            }
        });
    }


</script>

<body>
    广告列表：<select type="text" id="ads" name="ads" style="width: 300px"></select>
    <input type="button" value="测试" onclick="query()"/> <br/>
    <br/>
    <br/>
    <br/>

    <iframe src="" id="ad" frameborder="0" scrolling="no" marginheight="0" marginwidth="0" topmargin="0" leftmargin="0" allowtransparency="true" width="300" height="250">
    </iframe>

</body>
</html>