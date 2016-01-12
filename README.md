In [akka/akka#19403](https://github.com/akka/akka/pull/19403), all commits of the former development branch of
akka-stream and akka-http were duplicated before merging them into master.

For reasons explained in [akka/akka#19403](https://github.com/akka/akka/pull/19403), the duplicate history may lead to confusion over
what the real commit history of akka-stream/akka-http was. Also, it may interfere with the
ability to use git tools on branches introduced before the "merge" or git queries ranging
over commits before and after the merge.

For convenience, I provide an alternative merge commit that keeps the original history intact
but shouldn't otherwise interfere with your git checkout.

To make use of it, acquaint yourself with the functionality of [`git replace`](http://git-scm.com/book/en/v2/Git-Tools-Replace) and run this command in your akka checkout:

```
git fetch https://github.com/jrudolph/akka.git refs/replace/84fd5a9b2a4ebc1dcfe1cda8c072023ba84b44a0:refs/replace/84fd5a9b2a4ebc1dcfe1cda8c072023ba84b44a0
```

This will fetch my version of the merge commit and instruct git to use it instead of the original one.

You can revert this replacement at any time by running `git replace -d 84fd5a9b2a4ebc1dcfe1cda8c072023ba84b44a0`.
