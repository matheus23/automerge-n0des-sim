use std::{str::FromStr, time::Duration};

use anyhow::{Context as _, Result};
use automerge::transaction::Transactable;
use iroh::{Endpoint, protocol::Router};
use iroh_automerge_repo::IrohRepo;
use iroh_n0des::simulation::{Node, RoundContext, Spawn};
use samod::{
    DocHandle, DocumentId, PeerId, Samod,
    storage::{InMemoryStorage, Storage, StorageKey},
};
use tokio::{task::JoinSet, time::timeout};

struct AutomergeNode {
    repo: IrohRepo,
    router: Router,
    sync_tasks: JoinSet<Result<()>>,
}

impl AutomergeNode {
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }
}

impl Spawn<()> for AutomergeNode {
    async fn spawn(context: &mut iroh_n0des::simulation::SpawnContext<'_, ()>) -> Result<Self> {
        let ep = context.bind_endpoint().await?;
        let node_id = ep.node_id();
        let storage = InMemoryStorage::new();
        storage
            .put(
                StorageKey::from_parts(TEST_STORAGE_KEY),
                TEST_STORAGE_VALUE.to_vec(),
            )
            .await;
        let repo = Samod::build_tokio()
            .with_peer_id(PeerId::from_string(node_id.to_string()))
            .with_storage(storage)
            .load()
            .await;
        let repo = IrohRepo::new(ep.clone(), repo);

        let router = iroh::protocol::Router::builder(ep)
            .accept(IrohRepo::ALPN, repo.clone())
            .spawn();

        let sync_tasks = JoinSet::new();

        Ok(Self {
            repo,
            router,
            sync_tasks,
        })
    }
}

impl Node for AutomergeNode {
    async fn shutdown(&mut self) -> Result<()> {
        self.router.shutdown().await?;
        while let Some(result) = self.sync_tasks.join_next().await {
            match result {
                Err(e) => {
                    if let Ok(reason) = e.try_into_panic() {
                        std::panic::resume_unwind(reason);
                    }
                }
                Ok(result) => result?,
            }
        }
        Ok(())
    }
}

const TEST_DOCUMENT_ID: &str = "2ZiqrTNH7ReMNQxNnDy3e1pvt31D";
const TEST_STORAGE_KEY: &[&str] = &[
    TEST_DOCUMENT_ID,
    "incremental",
    "35328779586cc60cf02b1ed124e38b58b65c1ce191bb6da2b51d7358150cbfd4",
];
const TEST_STORAGE_VALUE: &[u8] = &[
    133, 111, 74, 131, 53, 50, 135, 121, 1, 24, 0, 16, 177, 240, 204, 48, 224, 65, 72, 26, 160,
    183, 93, 153, 175, 242, 233, 191, 1, 1, 0, 0, 0, 0,
];

impl AutomergeNode {
    async fn tick_bootstrap(&mut self, ctx: &RoundContext<'_, ()>) -> Result<bool> {
        for addr in ctx.all_other_nodes(self.endpoint().node_id()).cloned() {
            self.sync_tasks.spawn({
                let repo = self.repo.clone();
                async move {
                    repo.sync_with(addr).await?;
                    Ok(())
                }
            });
        }

        for addr in ctx.all_other_nodes(self.endpoint().node_id()) {
            self.repo
                .repo()
                .when_connected(PeerId::from_string(addr.node_id.to_string()))
                .await
                .expect("repo stopped unexpectedly");
        }

        Ok(true)
    }

    async fn tick_broadcast(&mut self, ctx: &RoundContext<'_, ()>) -> Result<bool> {
        let doc = self.get_test_doc().await?;

        doc.with_document(|doc| {
            doc.transact(|tx| {
                let prop = format!("node-{}-round-{}", ctx.node_index(), ctx.round());
                tx.put(automerge::ROOT, prop, "wahoo")?;
                anyhow::Ok(())
            })
        })
        .expect("failed to change document");

        tokio::time::sleep(Duration::from_secs(2)).await; // give samod some time to propagate the change

        Ok(true)
    }

    fn check(&self, _ctx: &RoundContext<'_, ()>) -> Result<()> {
        Ok(())
    }

    async fn get_test_doc(&self) -> Result<DocHandle> {
        let doc_id = DocumentId::from_str(TEST_DOCUMENT_ID).expect("failed to parse hardcoded");
        let doc = timeout(Duration::from_secs(1), self.repo.repo().find(doc_id))
            .await?
            .context("finding test doc timed out")?
            .context("missing doc")?;
        Ok(doc)
    }
}

#[iroh_n0des::sim]
async fn automerge_simulation() -> Result<iroh_n0des::simulation::Builder<()>> {
    async fn tick(node: &mut AutomergeNode, ctx: &RoundContext<'_, ()>) -> Result<bool> {
        match ctx.round() {
            0 => node.tick_bootstrap(ctx).await,
            _ => node.tick_broadcast(ctx).await,
        }
    }

    fn check(node: &AutomergeNode, ctx: &RoundContext<'_, ()>) -> Result<()> {
        node.check(ctx)
    }

    let sim = iroh_n0des::simulation::Builder::with_setup(|| async { Ok(()) })
        .spawn(8, AutomergeNode::builder(tick).check(check))
        .rounds(4);
    Ok(sim)
}
