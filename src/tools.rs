pub struct GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    slice: &'a [T],
    key: F,
}

impl<'a, T, F, K> GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    pub fn new(slice: &'a [T], key: F) -> Self {
        GroupBy { slice, key }
    }
}

impl<'a, T, F, K> Iterator for GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    type Item = (K, &'a [T]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.slice.is_empty() {
            None
        } else {
            let mut len = 1;
            let mut iter = self.slice.iter();
            let key = (self.key)(iter.next().unwrap());
            while let Some(x) = iter.next() {
                if key == (self.key)(x) {
                    len += 1;
                } else {
                    break;
                }
            }
            let (head, tail) = self.slice.split_at(len);
            self.slice = tail;
            Some((key, head))
        }
    }
}

pub struct ExactSizeIter<I: Iterator> {
    iter: I,
    len: usize,
}

impl<I: Iterator> ExactSizeIter<I> {
    pub fn new(iter: I, len: usize) -> Self {
        Self { iter, len }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for ExactSizeIter<I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I: Iterator> ExactSizeIterator for ExactSizeIter<I> {
    fn len(&self) -> usize {
        self.len
    }
}
