package com.baidu.hugegraph.api.gremlin;

import org.opencypher.gremlin.translation.TranslationFacade;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

@Path("graphspaces/{graphspace}/graphs/{graph}/cypher")
@Singleton
public class CypherAPI extends GremlinQueryAPI {

    private static final Logger LOG = Log.logger(CypherAPI.class);


    @GET
    @Timed
    @CompressInterceptor.Compress(buffer = (1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response query(@Context HttpHeaders headers,
                          @PathParam("graphspace") String graphspace,
                          @PathParam("graph") String graph,
                          @QueryParam("cypher") String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }

    @POST
    @Timed
    @CompressInterceptor.Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context HttpHeaders headers,
                         @PathParam("graphspace") String graphspace,
                         @PathParam("graph") String graph,
                         String cypher) {

        return this.queryByCypher(headers, graphspace, graph, cypher);
    }

    private Response queryByCypher(HttpHeaders headers, String graphspace,
                                   String graph, String cypher) {

        E.checkArgument(graphspace != null && !graphspace.isEmpty(),
                "The graphspace parameter can't be null or empty");
        E.checkArgument(graph != null && !graph.isEmpty(),
                "The graph parameter can't be null or empty");
        E.checkArgument(cypher != null && !cypher.isEmpty(),
                "The cypher parameter can't be null or empty");
        String gremlin = this.translateCypher2Gremlin(cypher);
        LOG.debug("translated gremlin is {}", gremlin);
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        String graphInfo = graphspace + "-" + graph;
        String gremlinQuery = "{"
                + "\"gremlin\":\"" + gremlin + "\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"graph\":" + "\"" + graphInfo + "\"" +
                ", \"g\":\"__g_" + graphInfo + "\"" + "}}";
        Response response = this.client().doPostRequest(auth, gremlinQuery);
        return transformResponseIfNeeded(response);
    }

    private String translateCypher2Gremlin(String cypher) {
        TranslationFacade translator = new TranslationFacade();
        String gremlin = translator.toGremlinGroovy(cypher);
        gremlin = this.buildQueryableGremlin(gremlin);
        return gremlin;
    }

    private String buildQueryableGremlin(String gremlin) {
        /*
         * `CREATE (a:person { name : 'test', age: 20) return a`
         * would be translated to:
         * `g.addV('person').as('a').property(single, 'name', 'test') ...`,
         * but hugegraph don't support `.property(single, k, v)`,
         * so we replace it to `.property(k, v)` here
         */
        gremlin = gremlin.replace(".property(single,", ".property(");

        return gremlin;
    }
}
