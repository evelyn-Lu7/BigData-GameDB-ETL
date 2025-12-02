"""将Markdown报告转换为HTML"""
import markdown
from markdown.extensions import codehilite, extra

# 读取Markdown文件
with open('eda_report.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# 转换为HTML
html_content = markdown.markdown(md_content, extensions=['extra', 'codehilite'])

# 创建完整的HTML文档
html_doc = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>游戏数据EDA分析报告</title>
    <style>
        body {{
            font-family: Arial, "Microsoft YaHei", sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 5px;
        }}
        h3 {{
            color: #7f8c8d;
            margin-top: 20px;
        }}
        img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 5px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        code {{
            background: #f4f4f4;
            padding: 2px 5px;
            border-radius: 3px;
            font-family: "Courier New", monospace;
        }}
        pre {{
            background: #f4f4f4;
            padding: 10px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #3498db;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 20px 0;
            padding-left: 20px;
            color: #7f8c8d;
        }}
    </style>
</head>
<body>
{html_content}
</body>
</html>"""

# 写入HTML文件
with open('eda_report.html', 'w', encoding='utf-8') as f:
    f.write(html_doc)

print("Markdown已成功转换为HTML: eda_report.html")

