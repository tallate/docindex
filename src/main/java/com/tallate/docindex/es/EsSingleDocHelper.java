package com.tallate.docindex.es;

import com.tallate.docindex.es.convert.EsDoc2ObjUtil;
import com.tallate.docindex.util.EsException;
import com.tallate.docindex.util.UtilException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * 单文档操作工具
 *
 * @author hgc
 */
@Component
public class EsSingleDocHelper extends BaseEsHelper {

    private IndexRequest getRequest(EsRequest esRequest, DocWriteRequest.OpType opType) throws UtilException {
        IndexRequest request = new IndexRequest(esRequest.getIdxName(), esRequest.getType(), esRequest.getDocId());
        request.timeout(esRequest.getTimeout());
        request.routing(esRequest.getRouting());
        request.parent(esRequest.getParent());
        request.setRefreshPolicy(esRequest.getRefreshPolicy());
        // 使用外部版本
        request.version(esRequest.getVersion());
        request.versionType(VersionType.EXTERNAL);
        // 操作类型
        request.opType(opType);
        request.setPipeline(esRequest.getPipeline());
        // 支持三种方式设置文档类型
        Object source = esRequest.getSource();
        if (source instanceof String) {
            request.source((String) source, XContentType.JSON);
        } else if (source instanceof Map) {
            request.source((Map) source);
        } else if (source instanceof XContentBuilder) {
            request.source((XContentBuilder) source);
        } else {
            throw new UtilException("不支持的参数类型");
        }
        return request;
    }

    private GetRequest getGetRequest(EsRequest esRequest) {
        GetRequest request = new GetRequest(esRequest.getIdxName(), esRequest.getType(), esRequest.getDocId());
        if (null != esRequest.getIncludes()) {
            String[] includes = esRequest.getIncludes();
            // 指定返回的字段，默认会返回_source
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);
            // 不返回source字段
            // request.fetchSourceContext(new FetchSourceContext(false));
            // TODO：下面这种方式也可以吗？？？
            // 指定返回的字段，默认会返回_source
            request.storedFields(includes);
        }

        // TODO：routing
        // request.routing("routing");
        // TODO：parent
        // request.parent("parent");
        // TODO：preference
        // request.preference("preference");
        // TODO：实时？？？默认是true
        // request.realtime(false);
        // 在获取文档前先刷新，总是获取最新的数据，默认false
        // TODO：有没有必要放到配置文件里？？？
        request.refresh(true);
        // TODO：version
        // request.version(2);
        // request.versionType(VersionType.EXTERNAL);
        return request;
    }

    private DeleteRequest getDeleteRequest(EsRequest esRequest) {
        DeleteRequest request = new DeleteRequest(esRequest.getIdxName(), esRequest.getType(), esRequest.getDocId());

        // TODO：routing
        // request.routing("routing");
        // TODO：parent
        // request.parent("parent");
        // 等待主分片可用的超时时间
        request.timeout(timeout);
        // 刷新策略
        request.setRefreshPolicy(refresh_policy);
        // TODO：version
        // request.version(2);
        // request.versionType(VersionType.EXTERNAL);
        return request;
    }

    /**
     * 文档更新请求
     */
    private UpdateRequest getUpdateRequest(EsRequest esRequest) throws UtilException {
        UpdateRequest request = new UpdateRequest(esRequest.getIdxName(), esRequest.getType(), esRequest.getDocId());

        // TODO: routing
        // request.routing("routing");
        // TODO: parent
        // request.parent("parent");
        // 等待主分片可用的超时时间
        request.timeout(timeout);
        // 刷新策略
        request.setRefreshPolicy(refresh_policy);
        // 更新操作由一次get和indexing组成，如果在这两个阶段之间文档被改变了、则更新操作会失败，这时候就需要重试
        request.retryOnConflict(3);
        // 指定返回的字段，默认会返回_source
        String[] includes = esRequest.getIncludes();
        if (null == includes) {
            request.fetchSource(true);
        } else {
            String[] excludes = Strings.EMPTY_ARRAY;
            request.fetchSource(new FetchSourceContext(true, includes, excludes));
        }
        // TODO: noop
        request.detectNoop(false);
        // 需要一定数量active分片才能执行
        request.waitForActiveShards(wait_for_active_shards);

        // inline脚本
        // Map<String, Object> parameters = singletonMap("count", 4); // 提供给脚本的参数
        // Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", parameters);
        // request.script(inline);
        // stored脚本
        // Script stored = new Script(ScriptType.STORED, null, "increment-field", parameters);
        // request.script(stored);
        // 如果文档不存在、脚本照样执行，比如脚本里已经考虑了不存在的情况、创建一个新的文档
        // request.scriptedUpsert(true);

        // 部分更新
        Object source = esRequest.getSource();
        if (source instanceof String) {
            request.doc((String) source, XContentType.JSON);
        } else if (source instanceof Map) {
            request.doc((Map) source);
        } else if (source instanceof XContentBuilder) {
            request.doc((XContentBuilder) source);
        } else {
            throw new UtilException("不支持的参数类型");
        }
        // Indicate that the partial document must be used as the upsert document if it does not exist yet.
        request.docAsUpsert(true);
        // 如果文档不存在，则使用upsert新增
        if (source instanceof String) {
            request.upsert((String) source, XContentType.JSON);
        } else if (source instanceof Map) {
            request.upsert((Map) source);
        } else if (source instanceof XContentBuilder) {
            request.upsert((XContentBuilder) source);
        } else {
            throw new UtilException("不支持的参数类型");
        }

        return request;
    }

    public String updateDocument(EsRequest esRequest) {
        try (RestHighLevelClient client = getClient()) {
            UpdateRequest updateRequest = getUpdateRequest(esRequest);
            UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
            return update.getId();
        } catch (IOException e) {
            throw new EsException("索引请求失败", e);
        }
    }

    public Map<String, Object> getDocument(EsRequest esRequest) {
        try (RestHighLevelClient client = getClient()) {
            GetRequest getRequest = getGetRequest(esRequest);
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            return response.getSourceAsMap();
        } catch (IOException e) {
            throw new EsException("读取文档失败", e);
        }
    }

    /**
     * 使用IndexRequest执行文档的创建和更新操作
     *
     * @param opType   操作类型
     * @param listener 监听器
     */
    public void doDocument(EsRequest esRequest, DocWriteRequest.OpType opType, ActionListener<IndexResponse> listener) throws UtilException, EsException {
        try (RestHighLevelClient client = getClient()) {
            IndexRequest request = getRequest(esRequest, opType);
            if (null == listener) {
                IndexResponse response = client.index(request);
                // 检查是否成功
                if (opType == DocWriteRequest.OpType.CREATE &&
                        response.getResult() == IndexResponse.Result.CREATED) {
                    // 第一次创建
                } else if (opType == DocWriteRequest.OpType.INDEX &&
                        (response.getResult() == IndexResponse.Result.UPDATED ||
                                response.getResult() == IndexResponse.Result.CREATED)) {
                    // INDEX操作同时支持新建和更新
                } else {
                    // TODO: 检查请求类型
                    throw new EsException("索引请求失败: opType[" + opType.getLowercase() + "] result[" + response.getResult().getLowercase() + "]");
                }
                // 检查分片是否成功
                ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    // TODO：为什么会出现不是所有分片都成功的情况？？？
                }
                if (shardInfo.getFailed() > 0) {
                    StringBuilder errs = new StringBuilder();
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        errs.append(failure.reason());
                    }
                    throw new EsException("部分分片出错: [全部]" + shardInfo.getTotal() +
                            " [成功]" + shardInfo.getSuccessful() + " [失败]" + shardInfo.getFailed() +
                            " [原因]" + errs);
                }
            } else {
                client.indexAsync(request, RequestOptions.DEFAULT, listener);
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                throw new EsException("版本冲突", e);
            }
            throw new EsException("索引请求失败", e);
        } catch (IOException e) {
            throw new EsException("索引请求失败", e);
        }
    }

    /**
     * 创建文档
     * 1. 原本使用DocWriteRequest.OpType.CREATE作为操作类型，
     * 但是它不支持EXTERNAL的version类型（创建文档没有必要使用版本号），最后统一使用INDEX了
     */
    public String createDocument(EsRequest esRequest, ActionListener<IndexResponse> listener) throws UtilException, EsException {
        this.doDocument(esRequest, DocWriteRequest.OpType.INDEX, listener);
        return esRequest.getDocId();
    }

    /**
     * 更新文档
     * 1. 多传入一个版本号
     */
    public String updateDocument(EsRequest esRequest, ActionListener<IndexResponse> listener) throws UtilException, EsException {
        this.doDocument(esRequest, DocWriteRequest.OpType.INDEX, listener);
        return esRequest.getDocId();
    }

    /**
     * 获取文档
     * 返回的Map：{"name": "Mike", "pwd": "123456"}
     */
    public <T> T getDocument(EsRequest esRequest, Class<T> clazz, ActionListener<GetResponse> listener)
            throws UtilException, EsException {
        try (RestHighLevelClient client = getClient()) {
            GetRequest request = getGetRequest(esRequest);
            if (null == listener) {
                GetResponse response = client.get(request);
                // 如果文档存在
                if (response.isExists()) {
                    // 这四个属性和请求里的一样
                    // String index = response.getIndex();
                    // String type = response.getType();
                    // String id = response.getId();
                    // long version = response.getVersion();
                    // TODO：getFields和getSource有什么区别？？？
                    Map<String, Object> fieldMap = response.getSourceAsMap();
                    // Map<String, DocumentField> fieldMap1 = response.getFields();
                    return EsDoc2ObjUtil.convert(fieldMap, esRequest.getIncludes(), clazz);
                } else {
                    // 就算响应码是404也不会抛出异常，所以这里要处理文档没找到的情况
                    throw new UtilException("文档不存在");
                }
            } else {
                client.getAsync(request, listener);
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                throw new UtilException("索引不存在", e);
            } else if (e.status() == RestStatus.CONFLICT) {
                throw new UtilException("文档已过期，请刷新", e);
            }
            throw new UtilException("获取文档请求失败", e);
        } catch (IOException e) {
            throw new UtilException("获取文档请求失败", e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new UtilException("反序列化对象失败", e);
        }
        return null;
    }

    /**
     * 删除文档
     */
    public void deleteDocument(EsRequest esRequest, ActionListener<DeleteResponse> listener) throws UtilException {
        try (RestHighLevelClient client = getClient()) {
            DeleteRequest request = getDeleteRequest(esRequest);
            if (null == listener) {
                DeleteResponse response = client.delete(request);
                if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    throw new UtilException("文档不存在");
                }
                ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
                // 处理部分分片不成功的情况
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    throw new UtilException("成功分片数<总分片数");
                }
                // 处理错误
                if (shardInfo.getFailed() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        sb.append(reason);
                    }
                    throw new UtilException("失败分片数不为0：" + sb);
                }
            } else {
                client.deleteAsync(request, listener);
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                throw new UtilException("索引不存在", e);
            } else if (e.status() == RestStatus.CONFLICT) {
                throw new UtilException("文档已过期，请刷新", e);
            }
            throw new UtilException("获取文档请求失败", e);
        } catch (IOException e) {
            throw new UtilException("获取文档请求失败", e);
        }
    }

    /**
     * 希望能同时具备create和update功能
     */
    public void saveDocument(EsRequest esRequest, ActionListener<UpdateResponse> listener) throws UtilException {
        try (RestHighLevelClient client = getClient()) {
            UpdateRequest request = getUpdateRequest(esRequest);
            if (null == listener) {
                UpdateResponse response = client.update(request);
                if (response.getResult() == DocWriteResponse.Result.CREATED) {
                    // 第一次创建，即upsert
                } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {

                } else if (response.getResult() == DocWriteResponse.Result.DELETED) {

                } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                    // 结果没有变化的情况，比如noop操作
                }
                GetResult result = response.getGetResult();
                // 如果request设置了fetchSource(true)，则更新请求会返回更新过后的文档
                if (result.isExists()) {
                    // String sourceAsString = result.sourceAsString();
                    // Map<String, Object> sourceAsMap = result.sourceAsMap();
                    // byte[] sourceAsBytes = result.source();
                } else {
                }
                // 分片异常
                ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
                // 处理部分分片不成功的情况
                if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                    throw new UtilException("成功分片数<总分片数");
                }
                // 处理错误
                if (shardInfo.getFailed() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                        String reason = failure.reason();
                        sb.append(reason);
                    }
                    throw new UtilException("失败分片数不为0：" + sb);
                }
            } else {
                client.updateAsync(request, listener);
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                throw new UtilException("索引不存在", e);
            } else if (e.status() == RestStatus.CONFLICT) {
                throw new UtilException("文档已过期，请刷新", e);
            }
            throw new UtilException("获取文档请求失败", e);
        } catch (IOException e) {
            throw new UtilException("获取文档请求失败", e);
        }
    }

}
