package stage

// func TestGraph_TraverseByDepth(t *testing.T) {
// 	Convey("Given a stage.Graph", t, func() {
// 		// g — s3 ———— s21 ————— s1
// 		//   — sA   |_ s22 __|
// 		s1 := NewGraph(Stage{Name: "Level1"}, nil)
// 		s21 := NewGraph(Stage{Name: "Level2-1"}, nil, s1)
// 		s22 := NewGraph(Stage{Name: "Level2-2"}, nil, s1)
// 		s3 := NewGraph(Stage{Name: "Level3"}, nil, s21, s22)
// 		sA := NewGraph(Stage{Name: "Level3A"}, nil)
// 		g := NewGraph(Stage{Name: "Level4"}, nil, s3, sA)
//
// 		err := g.TraverseByDepth(func(depth int, v []*Vertex) error {
// 			switch depth {
// 			case 3:
// 				So(v, ShouldHaveLength, 1)
// 				So(v[0].Name, ShouldEqual, "Level4")
// 			case 2:
// 				So(v, ShouldHaveLength, 2)
// 				So(v[0].Name, ShouldEqual, "Level3")
// 				So(v[1].Name, ShouldEqual, "Level3A")
// 			case 1:
// 				So(v, ShouldHaveLength, 2)
// 				So(v[0].Name, ShouldEqual, "Level2-1")
// 				So(v[1].Name, ShouldEqual, "Level2-2")
// 			case 0:
// 				So(v, ShouldHaveLength, 1)
// 				So(v[0].Name, ShouldEqual, "Level1")
// 			default:
// 				return errors.Errorf("invalid depth: %d", depth)
// 			}
// 			return nil
// 		})
// 		So(err, ShouldBeNil)
// 	})
// }
