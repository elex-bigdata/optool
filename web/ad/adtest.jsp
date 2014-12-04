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

    function loadAds(){
        $.ajax({
            url: 'ad',
            data: {"action":'ads'},
            type: 'post',
            dataType: 'json',
            success:function(data){
                $('#ads').empty();
                for(var k in data){
                    console.info(data[k] + ":" + k);
                    $("<option value='"+ k +"'>" + data[k] +"</option>").appendTo('#ads');
                }
                $('#ads').change(query());
            }
        });
    }

    function query(){

        var id = $("#ads").val();

        if(!id){
            return;
        }

        $('#ad').attr("src","ad?action=adtest&id="+id).reload();

/*        $("#content").html("");
        $.ajax({
            type: 'POST',
            url:"ad",
            data:'action=adtest&id=' + id,
            success: function(msg){
                $("#content").html(msg);
            }
        });*/
    }

</script>

<body>
    广告列表：<select type="text" id="ads" name="ads" style="width: 200px"></select>
    <br/>
    <br/>

    <iframe src="" id="ad" frameborder="0" scrolling="no" marginheight="0" marginwidth="0" topmargin="0" leftmargin="0" allowtransparency="true" width="300" height="250">
    </iframe>

</body>
</html>