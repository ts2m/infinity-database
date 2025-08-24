"""Operator for clustering data based on embeddings."""

from __future__ import annotations
from typing import Dict, Any
import numpy as np, json, math
from collections import deque, defaultdict
from sklearn.cluster import KMeans
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact
from pathlib import Path

@register
class AdaptiveCluster(Operator):
    name = "AdaptiveCluster"
    input_kinds = ["Embeddings"]
    output_kinds = ["ClusterMap"]

    def run(self, inputs: Dict[str, Artifact], initial_k: int=50, max_cluster_size: int=20,
            incremental_centroids_uri: str|None=None, workdir: str="", **_):
        emb = inputs["Embeddings"].data
        ids = emb["ids"]
        X = np.array(emb["vectors"], dtype=float)

        # 增量模式（可选）：已有质心 -> 直接分配新样本，超出阈值再细分
        if incremental_centroids_uri and Path(incremental_centroids_uri).exists():
            with open(incremental_centroids_uri, "r", encoding="utf-8") as f:
                centroids = np.array(json.load(f)["centroids"])
            # 直接最近质心分配
            assign = np.argmin(((X[:,None,:]-centroids[None,:,:])**2).sum(-1), axis=1)
            groups = defaultdict(list)
            for i, g in enumerate(assign):
                groups[int(g)].append(ids[i])
            # 对超限簇递归细分
            final_clusters = {}
            queue = deque(groups.values())
            while queue:
                grp = queue.popleft()
                if len(grp) <= max_cluster_size:
                    final_clusters[len(final_clusters)] = grp
                else:
                    k_new = math.ceil(len(grp)/max_cluster_size)
                    subX = X[[ids.index(gid) for gid in grp]]
                    km = KMeans(n_clusters=k_new, random_state=42, n_init="auto")
                    lab = km.fit_predict(subX)
                    for k in range(k_new):
                        subgrp = [grp[i] for i in range(len(grp)) if lab[i]==k]
                        queue.append(subgrp)
            # 保存质心（简单做法：重新用全部再拟合一遍）
            km_all = KMeans(n_clusters=min(initial_k, len(ids)), random_state=42, n_init="auto").fit(X)
            Path(incremental_centroids_uri).parent.mkdir(parents=True, exist_ok=True)
            json.dump({"centroids": km_all.cluster_centers_.tolist()}, open(incremental_centroids_uri,"w"))
        else:
            km = KMeans(n_clusters=min(initial_k, len(ids)), random_state=42, n_init="auto")
            labels = km.fit_predict(X)
            initial_groups = defaultdict(list)
            for i, lab in enumerate(labels):
                initial_groups[int(lab)].append(ids[i])

            final_clusters = {}
            queue = deque(initial_groups.values())
            while queue:
                grp = queue.popleft()
                if len(grp) <= max_cluster_size:
                    final_clusters[len(final_clusters)] = grp
                else:
                    k_new = math.ceil(len(grp)/max_cluster_size)
                    subX = X[[ids.index(gid) for gid in grp]]
                    km2 = KMeans(n_clusters=k_new, random_state=42, n_init="auto")
                    sub_labels = km2.fit_predict(subX)
                    for k in range(k_new):
                        subgrp = [grp[i] for i in range(len(grp)) if sub_labels[i]==k]
                        queue.append(subgrp)

            if incremental_centroids_uri:
                Path(incremental_centroids_uri).parent.mkdir(parents=True, exist_ok=True)
                json.dump({"centroids": km.cluster_centers_.tolist()}, open(incremental_centroids_uri,"w"))

        art = Artifact(kind="ClusterMap", data=final_clusters).save_json(f"{workdir}/cluster_map.json")
        return {"ClusterMap": art}
