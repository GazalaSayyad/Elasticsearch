from elasticsearch import helpers,Elasticsearch

es = Elasticsearch("http://localhost:9200")

def full_text_search(keyword, index, field):
    res = es.search(
        index=index,
        body={
            "query":{
                "match": {
                    field: keyword
                }
            }
        }
    )
    return res


def regex_search(pat, index, field):
    res = es.search(
        index=index,
        body={
            "query":{
                "regexp": {
                    field: pat
                }
            }
        }
    )
    return res


def prefix_search(prefix, index, field):
    res = es.search(
        index=index,
        body={
            "query":{
                "prefix": {
                    field: prefix
                }
            }
        }
    )
    return res


if __name__ == "__main__":
    index = "shakes"
    keyword = "Coriolanus"
    field = "text_entry"
    regex_pat = ".*?e.+"
    prefix = "no"
    res = full_text_search(keyword, index, field)
    res1 = regex_search(regex_pat, index, field)
    res2 = prefix_search(prefix, index, field)
    print(res1)
    print("----------------------")
    print(res2)