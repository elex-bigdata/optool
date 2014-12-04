var indexdata = 
[
    { text: '广告日志',isexpand:false, children: [
		{url:"ad/adhit.jsp",text:"日志查询"}
	]
    },
    { text: '工具箱', isexpand: false, children: [
		{ url: "op/uid.jsp", text: "UID转换" }
	]
    },
    { text: '系统检测', isexpand: false, children: [
        { url: "op/drill.jsp", text: "系统自检" },
        { url: "op/drill.jsp", text: "Drill" },
        { url: "op/dataloader.jsp", text: "DataLoder" },
        { url: "op/internet.jsp", text: "Internet" }
    ]
    }
];


var indexdata2 =
[
    { isexpand: "false", text: "Thor", children: [
        { isexpand: "false", text: "可排序", children: [
		    { url: "ad/adtest.jsp", text: "广告测试" }
	    ]
        },
        { isexpand: "true", text: "树表格", children: [
		    { url: "dotnetdemos/grid/treegrid/tree.aspx", text: "树表格" }, 
		    { url: "dotnetdemos/grid/treegrid/tree2.aspx", text: "树表格2" }
	    ]
        }
    ]
    }
];
