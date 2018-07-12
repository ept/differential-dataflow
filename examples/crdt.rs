extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::Collection;

type Id = (usize, usize);

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let (mut insert, mut remove, mut assign) = worker.dataflow::<_,_,_>(|scope| {

            // input handles for the three input collections.
            let (i_handle, insert) = scope.new_collection::<(Id, Id),isize>();
            let (r_handle, remove) = scope.new_collection::<Id,isize>();
            let (a_handle, assign) = scope.new_collection::<(Id, Id, String),isize>();

            let has_child = insert.map(|(_, x)| x).distinct();

            let insert_temp = insert.map(|(x,y)| (y,x));
            let later_child = insert_temp.join(&insert_temp)
                                         .filter(|&(k,x,y)| x > y)  // tuple order is lexicographic
                                         .map(|(k,x,y)| (k,y))
                                         .distinct();

            // NB: Correct only under assumption of distinctness from above.
            let first_child = insert_temp.concat(&later_child.negate());

            // NB: Common subcomputation with `later_child` above.
            let sibling = insert_temp.join(&insert_temp).map(|(_k,x,y)| (x,y));

            let later_sibling = sibling.filter(|&(x,y)| x > y);     // tuple order is lexicographic
            let later_sibling2 = later_sibling.map(|(x,y)| (y,x)).join(&later_sibling).map(|(y,x,z)| (x,z));

            // NB: Almost certainly a more efficient way to do this, e.g. `min`.
            let next_sibling = later_sibling.map(|x| (x,())).antijoin(&later_sibling2.distinct()).map(|(x,())| x);

            let has_next_sibling: Collection<_,Id,_> = next_sibling.map(|(x,_)| x).distinct();

            // NB: Done using `iterate` because no mutual recursion.
            let next_sibling_anc = next_sibling.iterate(|inner| {

                let insert = insert.enter(&inner.scope());
                let has_next_sibling = has_next_sibling.enter(&inner.scope());
                let next_sibling = next_sibling.enter(&inner.scope());

                insert.antijoin(&has_next_sibling)
                      .map(|(start, parent)| (parent, start))
                      .join(&inner)
                      .map(|(_parent, start, next)| (start, next))
                      .concat(&next_sibling)
                      .distinct()

            });

            let next_elem = first_child.concat(&next_sibling_anc.antijoin(&has_child));

            let current_value = assign.map(|(id, elem, value)| (id, (elem, value)))
                                      .antijoin(&remove)
                                      .map(|(_, (elem, value))| (elem, value));

            let has_value = current_value.map(|(elem,_)| elem).distinct(); // distinct optional?

            let skip_blank = next_elem.iterate(|inner| {

                let next_elem = next_elem.enter(&inner.scope());
                let has_value = has_value.enter(&inner.scope());

                next_elem.map(|(from, via)| (via, from))
                         .antijoin(&has_value)
                         .join(&inner)
                         .map(|(_via, from, to)| (from, to))
                         .concat(&next_elem)
                         .distinct()
            });

            let next_visible = skip_blank.semijoin(&has_value)
                                         .map(|(prev, next)| (next,prev))
                                         .semijoin(&has_value)
                                         .map(|(next,prev)| (prev,next));

            let result = next_visible.map(|(prev,next)| (next,prev))
                                     .join(&current_value)
                                     .map(|(next,prev,value)| (prev.0, next.0, value));
            next_elem.consolidate().inspect(|x| println!("{:?}", x));

            (i_handle, r_handle, a_handle)
        });

        insert.insert(((1,0), (0,0)));
        insert.insert(((2,0), (0,0)));
        insert.insert(((3,0), (2,0)));
        insert.insert(((4,0), (1,0)));
        insert.insert(((5,0), (2,0)));
        insert.insert(((6,0), (2,0)));

        /*
            assign([0,1], [0,0], "-"). // need to assign a dummy value to the head element?
            assign([2,1], [2,0], "H").
            assign([6,1], [6,0], "e"). assign([6,2], [6,0], "i"). remove([6,2]).
            assign([5,1], [5,0], "l"). remove([5,1]). assign([5,3], [5,0], "y").
            assign([3,1], [3,0], "l"). remove([3,1]).
            assign([1,1], [1,0], "o"). assign([1,2], [1,0], "i"). remove([1,1]). remove([1,2]).
            assign([4,1], [4,0], "!"). assign([4,2], [4,0], "?").
            */
        insert.advance_to(1); insert.flush();
        remove.advance_to(1); remove.flush();
        assign.advance_to(1); assign.flush();


    }).unwrap();
}
