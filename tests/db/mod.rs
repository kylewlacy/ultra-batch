use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use ultra_batch::{Cache, Executor, Fetcher};
use uuid::Uuid;

pub struct Database {
    pub users: HashMap<Uuid, User>,
    pub posts: HashMap<Uuid, Post>,
    pub comments: HashMap<Uuid, Comment>,
}

impl Database {
    pub fn fake() -> Self {
        let users: HashMap<_, _> = (0..1000)
            .map(|_| User::fake())
            .map(|user| (user.id, user))
            .collect();
        let posts: HashMap<_, _> = users
            .values()
            .enumerate()
            .flat_map(|(n, user)| (0..(n % 3)).map(move |_| Post::fake(user.id)))
            .map(|post| (post.id, post))
            .collect();
        let comments: HashMap<_, _> = posts
            .values()
            .enumerate()
            .flat_map(|(n, post)| {
                let commenter = users.values().nth(n % users.len()).unwrap();
                (0..3).map(move |_| Comment::fake(post.id, commenter.id))
            })
            .map(|comment| (comment.id, comment))
            .collect();

        let db = Database {
            users,
            posts,
            comments,
        };
        db
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub id: Uuid,
    pub name: String,
}

impl User {
    pub fn fake() -> Self {
        Self {
            id: Uuid::new_v4(),
            name: fakeit::name::full(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Post {
    pub id: Uuid,
    pub user_id: Uuid,
    pub body: String,
}

impl Post {
    pub fn fake(user_id: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            user_id,
            body: fakeit::words::sentence(3),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comment {
    pub id: Uuid,
    pub post_id: Uuid,
    pub user_id: Uuid,
    pub comment: String,
}

impl Comment {
    pub fn fake(post_id: Uuid, user_id: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            post_id,
            user_id,
            comment: fakeit::words::sentence(2),
        }
    }
}

pub struct FetchUsers {
    pub db: Arc<RwLock<Database>>,
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
        let db = self
            .db
            .read()
            .map_err(|_| anyhow::anyhow!("failed to lock database"))?;
        for key in keys {
            if let Some(user) = db.users.get(key) {
                values.insert(*key, user.clone())
            }
        }

        Ok(())
    }
}

pub struct InsertUsers {
    pub db: Arc<RwLock<Database>>,
}

impl Executor for InsertUsers {
    type Value = User;
    type Result = Option<Uuid>;
    type Error = anyhow::Error;

    async fn execute(&self, values: Vec<User>) -> Result<Vec<Option<Uuid>>, Self::Error> {
        let mut results = Vec::with_capacity(values.len());

        let mut db = self
            .db
            .write()
            .map_err(|_| anyhow::anyhow!("failed to lock database"))?;
        for value in values {
            let result = match db.users.entry(value.id) {
                std::collections::hash_map::Entry::Occupied(_) => None,
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let id = value.id;
                    entry.insert(value);
                    Some(id)
                }
            };
            results.push(result);
        }

        Ok(results)
    }
}
