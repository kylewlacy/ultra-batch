use std::{collections::HashMap, sync::Arc};
use ultra_batch::{Cache, Fetcher};
use uuid::Uuid;

pub struct Database {
    pub users: HashMap<Uuid, User>,
    pub posts: HashMap<Uuid, Post>,
    pub comments: HashMap<Uuid, Comment>,
}

impl Database {
    pub fn fake() -> Arc<Self> {
        let users: HashMap<_, _> = (0..1000)
            .map(|_| User {
                id: Uuid::new_v4(),
                name: fakeit::name::full(),
            })
            .map(|user| (user.id, user))
            .collect();
        let posts: HashMap<_, _> = users
            .values()
            .enumerate()
            .flat_map(|(n, user)| {
                (0..(n % 3)).map(move |_| Post {
                    id: Uuid::new_v4(),
                    user_id: user.id,
                    body: fakeit::words::sentence(3),
                })
            })
            .map(|post| (post.id, post))
            .collect();
        let comments: HashMap<_, _> = posts
            .values()
            .enumerate()
            .flat_map(|(n, post)| {
                let commenter = users.values().nth(n % users.len()).unwrap();
                (0..3).map(move |_| Comment {
                    id: Uuid::new_v4(),
                    post_id: post.id,
                    user_id: commenter.id,
                    comment: fakeit::words::sentence(2),
                })
            })
            .map(|comment| (comment.id, comment))
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

impl Fetcher for FetchUsers {
    type Key = Uuid;
    type Value = User;
    type Error = anyhow::Error;

    async fn fetch(
        &self,
        keys: &[Uuid],
        values: &mut Cache<'_, Uuid, User>,
    ) -> Result<(), Self::Error> {
        for key in keys {
            if let Some(user) = self.db.users.get(key) {
                values.insert(*key, user.clone())
            }
        }

        Ok(())
    }
}
