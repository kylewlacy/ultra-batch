use async_trait::async_trait;
use std::sync::Arc;
use ultra_batch::{Cache, Fetcher};
use uuid::Uuid;

pub struct Database {
    pub users: Vec<User>,
    pub posts: Vec<Post>,
    pub comments: Vec<Comment>,
}

impl Database {
    pub fn fake() -> Arc<Self> {
        let users: Vec<_> = (0..1000)
            .map(|_| User {
                id: Uuid::new_v4(),
                name: fakeit::name::full(),
            })
            .collect();
        let posts: Vec<_> = users
            .iter()
            .enumerate()
            .flat_map(|(n, user)| {
                (0..(n % 3)).map(move |_| Post {
                    id: Uuid::new_v4(),
                    user_id: user.id,
                    body: fakeit::words::sentence(3),
                })
            })
            .collect();
        let comments: Vec<_> = posts
            .iter()
            .enumerate()
            .flat_map(|(n, post)| {
                let commenter = &users[n % users.len()];
                (0..3).map(move |_| Comment {
                    id: Uuid::new_v4(),
                    post_id: post.id,
                    user_id: commenter.id,
                    comment: fakeit::words::sentence(2),
                })
            })
            .collect();

        let db = Database {
            users,
            posts,
            comments,
        };
        Arc::new(db)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Post {
    pub id: Uuid,
    pub user_id: Uuid,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comment {
    pub id: Uuid,
    pub post_id: Uuid,
    pub user_id: Uuid,
    pub comment: String,
}

pub struct FetchUsers {
    pub db: Arc<Database>,
}

#[async_trait]
impl Fetcher for FetchUsers {
    type Key = Uuid;
    type Value = User;
    type Error = anyhow::Error;

    async fn fetch(
        &self,
        keys: &[Self::Key],
        values: &Cache<Self::Key, Self::Value>,
    ) -> Result<(), Self::Error> {
        for key in keys {
            if let Some(user) = self.db.users.iter().find(|user| user.id == *key) {
                values.insert(*key, user.clone())
            }
        }

        Ok(())
    }
}
