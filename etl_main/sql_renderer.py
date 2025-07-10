from jinja2 import Template

def render_query(query_path, context):
    with open(f"sql_queries/{query_path}", "r") as f:
        template = Template(f.read())
    return template.render(**context)

