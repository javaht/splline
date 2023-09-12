from flask import Flask, request
import json
from datahub.metadata.schema_classes import ChangeTypeClass
from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_post_request():
    sqldata = request.get_data()
    sqldata = sqldata.decode()
    try:
        data = json.loads(sqldata)
    except Exception as e:
        return "Invalid JSON", 400

    sql = data['sql']
    targedaBase = data['targedaBase']
    targeTable =  data['targeTable']
    endsql = 'insert into ' + targedaBase + '.' + targeTable + ' '+sql+'  '
    process_sql(endsql)

    return '接口收到：sql：'+endsql+' ;Database：'+targedaBase+'  ;Table：'+targeTable+' '

def process_sql(endsql):
    if endsql:
        result = LineageRunner(endsql)
        targetTableName = result.target_tables[0].__str__()  # 获取sql中的下游表名
        lineage = result.get_column_lineage   # 获取列级血缘
        result.print_column_lineage() # 打印列级血缘结果
        baozhuang(result,targetTableName,lineage)

# 库名设置
def datasetUrn(tableName):
    return builder.make_dataset_urn("hive", tableName)  # platform = hive

# 表、列级信息设置
def fieldUrn(tableName, fieldName):
    return builder.make_schema_field_urn(datasetUrn(tableName), fieldName)

def  baozhuang(result,targetTableName,lineage):
    # 字段级血缘list
    fineGrainedLineageList = []
    # 用于冲突检查的上游list
    upStreamsList = []

    # 遍历列级血缘
    for columnTuples in lineage():
        # 上游list
        upStreamStrList = []
        # 下游list
        downStreamStrList = []
        # 逐个字段遍历
        for column in columnTuples:
            # 元组中最后一个元素为下游表名与字段名，其他元素为上游表名与字段名
            # 遍历到最后一个元素，为下游表名与字段名
            if columnTuples.index(column) == len(columnTuples) - 1:
                downStreamFieldName = column.raw_name.__str__()
                downStreamTableName = column.__str__().replace('.' + downStreamFieldName, '').__str__()

                downStreamStrList.append(fieldUrn(downStreamTableName, downStreamFieldName))
            else:
                upStreamFieldName = column.raw_name.__str__()
                upStreamTableName = column.__str__().replace('.' + upStreamFieldName, '').__str__()
                upStreamStrList.append(fieldUrn(upStreamTableName, upStreamFieldName))
                # 用于检查上游血缘是否冲突
                upStreamsList.append(
                    Upstream(dataset=datasetUrn(upStreamTableName), type=DatasetLineageType.TRANSFORMED))

        fineGrainedLineage = FineGrainedLineage(upstreamType=FineGrainedLineageUpstreamType.DATASET,
                                                upstreams=upStreamStrList,

                                                downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                                                downstreams=downStreamStrList)

        fineGrainedLineageList.append(fineGrainedLineage)
        sendLine(targetTableName,upStreamsList,fineGrainedLineageList)

def sendLine(targetTableName,upStreamsList,fineGrainedLineageList):
    fieldLineages = UpstreamLineage(upstreams=upStreamsList, fineGrainedLineages=fineGrainedLineageList)
    lineageMcp = MetadataChangeProposalWrapper(
        entityUrn=datasetUrn(targetTableName),  # 下游表名
        aspect=fieldLineages,
        changeType=ChangeTypeClass.UPSERT,
        aspectName="upstreamLineage",
    )

    # 调用datahub REST API
    emitter = DatahubRestEmitter('http://172.18.1.54:18080', token="eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjYxMDVhMGVkLWNhZjctNDE5NC1hYmMzLTQ2NGU0ZDI5YTc1NSIsInN1YiI6ImRhdGFodWIiLCJpc3MiOiJkYXRhaHViLW1ldGFkYXRhLXNlcnZpY2UifQ.tFScljRefDvhOLM9pIQbjMIFw_HmkkYEcwlpFgNDLck")  # datahub gms server
    emitter.emit_mcp(lineageMcp)


if __name__ == '__main__':
    app.run(port=5620)


