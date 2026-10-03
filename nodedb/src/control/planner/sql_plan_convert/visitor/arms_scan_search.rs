// SPDX-License-Identifier: BUSL-1.1
//! `PlanVisitor` method bodies for timeseries/vector/text/hybrid/spatial search variants
//! on `ConvertVisitor`. Defined as a macro and invoked once from `adapter.rs`.

macro_rules! impl_scan_search_arms_for_convert_visitor {
    () => {
        fn timeseries_scan(
            &mut self,
            args: nodedb_sql::TimeseriesScanVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::TimeseriesScanVisitArgs {
                collection,
                time_range,
                bucket_interval_ms,
                group_by,
                aggregates,
                filters,
                projection,
                gap_fill,
                limit,
                sort_keys,
                tiered,
                temporal,
            } = args;
            super::super::scan::convert_timeseries_scan(
                super::super::scan_params::TimeseriesScanParams {
                    collection,
                    time_range: &time_range,
                    bucket_interval_ms: &bucket_interval_ms,
                    group_by,
                    aggregates,
                    filters,
                    projection,
                    gap_fill,
                    limit: &limit,
                    sort_keys,
                    tiered: &tiered,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                    temporal,
                },
            )
        }

        fn timeseries_ingest(
            &mut self,
            collection: &str,
            rows: &[Vec<(String, nodedb_sql::types_expr::SqlValue)>],
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_timeseries_ingest(
                collection,
                rows,
                self.tenant_id,
                self.ctx,
            )
        }

        fn vector_search(
            &mut self,
            args: nodedb_sql::VectorSearchVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::VectorSearchVisitArgs {
                collection,
                field,
                query_vector,
                top_k,
                ef_search,
                metric,
                filters,
                array_prefilter,
                ann_options,
                skip_payload_fetch,
                payload_filters,
                pk_prefilter,
            } = args;
            super::super::scan::convert_vector_search(
                super::super::scan_params::VectorSearchParams {
                    collection,
                    field,
                    query_vector,
                    top_k: &top_k,
                    ef_search: &ef_search,
                    metric: &metric,
                    filters,
                    array_prefilter,
                    ann_options,
                    tenant_id: self.tenant_id,
                    ctx: self.ctx,
                    skip_payload_fetch,
                    payload_filters,
                    pk_prefilter,
                },
            )
        }

        fn sparse_search(
            &mut self,
            collection: &str,
            field: &str,
            query_entries: &[(u32, f32)],
            top_k: usize,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            super::super::scan::convert_sparse_search(
                super::super::scan_params::SparseSearchParams {
                    collection,
                    field,
                    query_entries,
                    top_k: &top_k,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn text_search(
            &mut self,
            args: nodedb_sql::TextSearchVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::TextSearchVisitArgs {
                collection,
                shape,
                filters,
                scores,
            } = args;
            super::super::scan::convert_text_search(
                super::super::scan_params::TextSearchConvertParams {
                    collection,
                    shape,
                    filters,
                    scores,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn hybrid_search(
            &mut self,
            args: nodedb_sql::HybridSearchVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::HybridSearchVisitArgs {
                collection,
                vector_field,
                query_vector,
                text_field,
                query_text,
                filters,
                top_k,
                ef_search,
                vector_weight,
                mode,
                fuzzy,
                score_alias,
            } = args;
            super::super::scan::convert_hybrid_search(
                super::super::scan_params::HybridSearchParams {
                    collection,
                    vector_field,
                    query_vector,
                    text_field,
                    query_text,
                    filters,
                    top_k: &top_k,
                    ef_search: &ef_search,
                    vector_weight: &vector_weight,
                    mode,
                    fuzzy: &fuzzy,
                    score_alias,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn hybrid_search_triple(
            &mut self,
            args: nodedb_sql::HybridSearchTripleVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::HybridSearchTripleVisitArgs {
                collection,
                vector_field,
                query_vector,
                text_field,
                query_text,
                filters,
                graph_seed_id,
                graph_depth,
                graph_edge_label,
                top_k,
                ef_search,
                mode,
                fuzzy,
                rrf_k,
                score_alias,
            } = args;
            let graph_edge_label_owned: Option<String> = graph_edge_label.map(str::to_owned);
            super::super::scan::convert_hybrid_search_triple(
                super::super::scan_params::HybridSearchTripleParams {
                    collection,
                    vector_field,
                    query_vector,
                    text_field,
                    query_text,
                    filters,
                    graph_seed_id,
                    graph_depth: &graph_depth,
                    graph_edge_label: &graph_edge_label_owned,
                    top_k: &top_k,
                    ef_search: &ef_search,
                    mode,
                    fuzzy: &fuzzy,
                    rrf_k: &rrf_k,
                    score_alias,
                    tenant_id: self.tenant_id,
                    database_id: self.ctx.database_id,
                },
            )
        }

        fn spatial_scan(
            &mut self,
            args: nodedb_sql::SpatialScanVisitArgs<'_>,
        ) -> crate::Result<Vec<nodedb_physical::physical_task::PhysicalTask>> {
            let nodedb_sql::SpatialScanVisitArgs {
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters,
                attribute_filters,
                limit,
                projection,
            } = args;
            super::super::scan::convert_spatial_scan(super::super::scan_params::SpatialScanParams {
                collection,
                field,
                predicate,
                query_geometry,
                distance_meters: &distance_meters,
                attribute_filters,
                limit: &limit,
                projection,
                tenant_id: self.tenant_id,
                database_id: self.ctx.database_id,
            })
        }
    };
}

pub(super) use impl_scan_search_arms_for_convert_visitor;
